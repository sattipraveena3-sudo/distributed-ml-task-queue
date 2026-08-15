import json,math,sqlite3,time
from pathlib import Path

POSITIVE={"good","great","excellent","love","fast","helpful"}; NEGATIVE={"bad","poor","hate","slow","broken","awful"}
def classify(text):
    words=set(text.lower().split()); score=len(words&POSITIVE)-len(words&NEGATIVE); return {"label":"positive" if score>0 else "negative" if score<0 else "neutral","score":1/(1+math.exp(-abs(score)))}

def scaling_decision(queue_depth:int,workers:int,min_workers=1,max_workers=8,up_per_worker=5,down_threshold=1):
    if queue_depth>workers*up_per_worker and workers<max_workers: return min(max_workers,workers+max(1,math.ceil(queue_depth/up_per_worker)-workers))
    if queue_depth<=down_threshold and workers>min_workers: return workers-1
    return workers

class Metrics:
    def __init__(self,path="data/metrics.db"):
        Path(path).parent.mkdir(parents=True,exist_ok=True); self.path=path
        with sqlite3.connect(path) as c: c.execute("create table if not exists metrics(ts real,queue_depth int,workers int,throughput real,p95 real,event text)")
    def add(self,depth,workers,throughput=0,p95=0,event="sample"):
        with sqlite3.connect(self.path) as c:c.execute("insert into metrics values(?,?,?,?,?,?)",(time.time(),depth,workers,throughput,p95,event))
    def list(self):
        with sqlite3.connect(self.path) as c:c.row_factory=sqlite3.Row; return [dict(x) for x in c.execute("select * from metrics order by ts desc limit 300")]
