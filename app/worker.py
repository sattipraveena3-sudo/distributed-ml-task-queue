import json,os,random,time
import redis
from app.core import classify
r=redis.Redis.from_url(os.getenv('REDIS_URL','redis://redis:6379'),decode_responses=True)
while True:
    item=r.blpop('jobs',timeout=5)
    if not item: continue
    job=json.loads(item[1]); started=time.time(); time.sleep(.15+random.random()*.5); output=classify(job['text']); r.set('result:'+job['id'],json.dumps({'status':'complete','output':output,'latency':time.time()-started}),ex=3600); r.incr('completed')
