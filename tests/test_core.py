from app.core import Metrics,classify,scaling_decision
def test_job(): assert classify('excellent and helpful')['label']=='positive'
def test_scale_up_down(): assert scaling_decision(30,1)>1 and scaling_decision(0,3)==2
def test_metrics(tmp_path):
    m=Metrics(tmp_path/'m.db');m.add(7,2);assert m.list()[0]['queue_depth']==7
