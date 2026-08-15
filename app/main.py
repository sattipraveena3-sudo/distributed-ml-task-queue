import json,os,time,uuid
from pathlib import Path
import redis
from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from app.core import Metrics,scaling_decision
r=redis.Redis.from_url(os.getenv("REDIS_URL","redis://localhost:6379"),decode_responses=True); metrics=Metrics(); app=FastAPI(title="Distributed ML Task Queue"); static=Path(__file__).parent/"static"; app.mount('/static',StaticFiles(directory=static),name='static')
class Job(BaseModel): text:str
@app.get('/')
def home():return FileResponse(static/'index.html')
@app.get('/health')
def health(): return {'status':'ok'}
@app.post('/jobs')
def submit(job:Job):
    job_id=str(uuid.uuid4()); r.rpush('jobs',json.dumps({'id':job_id,'text':job.text,'submitted':time.time()})); return {'id':job_id,'status':'queued'}
@app.get('/jobs/{job_id}')
def result(job_id:str): return json.loads(r.get('result:'+job_id) or json.dumps({'status':'pending'}))
@app.get('/metrics')
def metric_data(): return metrics.list()
@app.post('/scale/sample')
def sample(workers:int=1):
    depth=r.llen('jobs'); target=scaling_decision(depth,workers); metrics.add(depth,target,event='scale' if target!=workers else 'sample'); return {'queue_depth':depth,'current':workers,'target':target,'mode':'decision simulation; Compose replicas are operator-managed'}
