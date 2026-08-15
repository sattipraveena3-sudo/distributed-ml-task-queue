import argparse,random,time,requests

def main():
    p=argparse.ArgumentParser();p.add_argument('--url',default='http://localhost:8000');p.add_argument('--jobs',type=int,default=80);a=p.parse_args()
    samples=['excellent and fast','bad and broken','helpful service','slow response']
    for i in range(a.jobs):
        requests.post(a.url+'/jobs',json={'text':random.choice(samples)},timeout=5)
        time.sleep(.02 if i%25<20 else .4)
    print(f'queued {a.jobs} bursty jobs')

if __name__=='__main__': main()
