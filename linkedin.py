# This file crawls job postings on LinkedIn for the query 'software engineer'
# and stores the salary and skills required for each job.

from bs4 import BeautifulSoup
import time 
import pymongo
import random
from datetime import datetime
import os

from urllib.parse import urlparse
from urllib.parse import parse_qs
from urllib.parse import quote

from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

import certifi
ca = certifi.where()

chrome_options = Options()
chrome_options.add_argument('—-ignore-certificate-errors')
chrome_options.add_argument('--user-data-dir=C:/Users/jhua8/AppData/Local/Google/Chrome/User Data') # Get logged in data
driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=chrome_options)

client = pymongo.MongoClient(os.environ.get('MONGODB_CONN'), tlsCAFile=ca)
db = client['indem']


def log_error(err_str):
    print('ERR: ' + err_str)
    with open('linkedin.log', 'a') as f:
        f.write('\nERR: ' + err_str)

# skills are separated with commas and 'and' before the last one
def parse_skills(skills_str, job_title):
    # replace 'and' with '&' in skills
    replacements = {
        'Continuous Integration and Continuous Delivery (CI/CD)': 'Continuous Integration & Continuous Delivery (CI/CD)',
        'Modeling and Simulation': 'Modeling & Simulation',
    }
    for k, v in replacements.items():
        skills_str = skills_str.replace(k, v)

    if len(skills_str.split(' and ')) > 2:
        log_error('more than 1 "and" in skills_str: ' + skills_str + ' for job title: ' + job_title)

    if ', and ' in skills_str: # more than 2 skills
        return skills_str.replace(' and ', ' ').split(', ')
    else: # 2 or less skills
        return skills_str.split(' and ')

def crawler(title):
    job_start_time = datetime.now().strftime("%Y%m%d %H:%M:%S")
    counter = 0
    jobs_inserted = 0
    jobs_not_inserted = 0
    skills_crawled = 0
    log_error('new error log for title ' + title + ' start time: ' + job_start_time)
    
    db_suffix = title.replace(' ', '_')
    jobs_linkedin = db['jobs_li_' + db_suffix]
    skills_linkedin = db['skills_li_' + db_suffix]
    crawler_log = db['crawler_log']

    # LI stops loading jobs after 1000
    while counter < 1000:
        driver.get('https://www.linkedin.com/jobs/search/?keywords=' + quote(title) + '&start=' + str(counter))
        time.sleep(5 + random.random() * 3)
        soup = BeautifulSoup(driver.page_source, 'html.parser')

        for link in soup.find_all('a', {'class': 'job-card-list__title'}):
            try:
                link_id = link.get('id')
                link_el = driver.find_element(By.ID, link_id)
                link_el.click()
                time.sleep(2 + random.random() * 2)

                parsed_url = urlparse(driver.current_url)
                job_id = parse_qs(parsed_url.query)['currentJobId'][0]
                job_title = driver.find_element(By.CLASS_NAME, 'job-details-jobs-unified-top-card__job-title').find_element(By.TAG_NAME, 'a').text

                skills = []
                skills_on_profile = []
                skills_missing = []

                try:
                    skills_container_alt = driver.find_element(By.CLASS_NAME, 'job-details-how-you-match__skills-section-descriptive-skill')
                    skills = skills_container_alt.text.split(' · ')
                except:
                    skills_containers = driver.find_elements(By.CLASS_NAME, 'job-details-how-you-match__skills-item-subtitle')
                    skills_on_profile = parse_skills(skills_containers[0].text, job_title)
                    skills_missing = parse_skills(skills_containers[1].text, job_title)

                salary_min = -1
                salary_max = -1
                pay_el_text = ''
                pay = []

                # sometimes pay_container will exist but will not have a salary
                # so getting pay_el_text will throw an error
                try:
                    pay_container = driver.find_element(By.CLASS_NAME, 'jobs-details__salary-main-rail-card')

                    # some jobs have no salary
                    if pay_container.size['height'] > 0:
                        pay_el_text = pay_container.find_element(By.TAG_NAME, 'div').find_element(By.TAG_NAME, 'div').find_elements(By.TAG_NAME, 'div')[1].find_element(By.TAG_NAME, 'p').text
                        pay = pay_el_text.split(' - ')

                        if len(pay) > 1:
                            if ' ' in pay[1]:
                                pay[1] = pay[1][:pay[1].index(' ')]
                            if '/yr' in pay[0]:
                                salary_min = int(pay[0][1:pay[0].index('/')].replace(',', ''))
                            if '/yr' in pay[1]:
                                salary_max = int(pay[1][1:pay[1].index('/')].replace(',', ''))
                        elif len(pay) == 1:
                            if 'Starting at $' in pay[0]:
                                salary_min = int(pay[0][len('Starting at $'):pay[0].index('/')].replace(',', ''))
                except Exception as e:
                    log_error('could not get salary from string: ' + pay_el_text + '. e: ' + str(e))
                
                # check if this job was already parsed
                if jobs_linkedin.find_one({'job_id': job_id}) is None:
                    final_skills = skills_on_profile + skills_missing + skills

                    job = {
                        'job_id': job_id,
                        'job_title': job_title,
                        'skills': final_skills,
                        'inserted_at': job_start_time
                    }

                    if salary_min != -1:
                        job['salary_min'] = salary_min
                    if salary_max != -1:
                        job['salary_max'] = salary_max

                    jobs_linkedin.insert_one(job)
                    jobs_inserted += 1

                    # update count for each skill found in this job description
                    for s in final_skills:
                        skills_crawled += 1

                        skill_db = skills_linkedin.find_one({'skill': s})
                        if skill_db is None:
                            skills_linkedin.insert_one({
                                'skill': s,
                                'count': 1,
                                'last_updated': job_start_time
                            })
                        else:
                            skills_linkedin.update_one(
                                {'skill': s },
                                { '$set': {
                                    'count': skill_db['count'] + 1,
                                    'last_updated': job_start_time
                            }})
                else:
                    log_error('job ' + job_id + ' already exists')

                
            except Exception as e:
                log_error('could not parse job ' + job_id + '. e: ' + str(e))
                jobs_not_inserted += 1
            
            counter += 1
        
    # keep track of crawler jobs
    crawler_log.insert_one({
        'job_start_time': job_start_time,
        'job_end_time': datetime.now().strftime("%Y%m%d %H:%M:%S"),
        'jobs_inserted': jobs_inserted,
        'jobs_not_inserted': jobs_not_inserted,
        'skills_crawled': skills_crawled,
        'title': title
    })
    
    print('jobs inserted: ' + str(jobs_inserted) + ', skills crawled: ' + str(skills_crawled) + ', jobs not inserted: ' + str(jobs_not_inserted))

    driver.close()


print('Starting the crawler...')
time.sleep(20)

titles = ['frontend engineer', 'backend engineer', 'full stack engineer', 'machine learning engineer']
for title in titles:
    crawler(title)