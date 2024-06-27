# a place for retroactively updating dbs
# a place for regrets
import pymongo
import os
import certifi
ca = certifi.where()

client = pymongo.MongoClient(os.environ.get('MONGODB_CONN'), tlsCAFile=ca)
db = client['indem']
JOBS_COLLECTION_PREFIX = 'jobs_li_'
SKILLS_COLLECTION_PREFIX = 'skills_li_'


def update_has_salary_data():
    titles = ['frontend_engineer', 'backend_engineer',
              'fullstack_engineer', 'machine_learning_engineer']
    hsd = 0
    nsd = 0

    for title in titles:
        skills_db = db[SKILLS_COLLECTION_PREFIX + title]
        jobs_db = db[JOBS_COLLECTION_PREFIX + title]
        skills = skills_db.find({})
        for s in skills:
            print(s['skill'])
            jobs_with_salary_data_count = jobs_db.count_documents(
                {'salary_min': {'$exists': True}, 'salary_max': {'$exists': True}, 'skills': s['skill']})
            if jobs_with_salary_data_count > 0:
                skills_db.update_one({
                    'skill': s['skill']
                }, {'$set': {'has_salary_data': True}})
                print('updated')
                hsd += 1
            else:
                print('no data :(')
                skills_db.update_one({
                    'skill': s['skill']
                }, {'$set': {'has_salary_data': False}})
                nsd += 1
    print(str(hsd) + ' with data')
    print(str(nsd) + ' with no data')


update_has_salary_data()
