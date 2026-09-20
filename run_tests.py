"""Reproducible deterministic checks. Run: python run_tests.py [output_dir]

All quantities, timestamps and concurrency settings are test inputs, not production
data, SLAs or observed customer traffic. Only outcomes are experimental observations.
"""
from __future__ import annotations
import csv, hashlib, json, platform, sqlite3, sys, tempfile
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from ledger import Ledger

OUT=Path(sys.argv[1]) if len(sys.argv)>1 else Path(__file__).parent/'results'
OUT.mkdir(parents=True,exist_ok=True)
records=[]

def evt(kind:str,id:str,at:int=0,c:str='C',f:str='F',source:str='test',**kw:Any)->dict[str,Any]:
    return {'kind':kind,'id':id,'at':at,'customer':c,'feature':f,'source':source,**kw}

def grant(l:Ledger,id:str='g',amount:int=100,priority:int=1,at:int=0,expires:int=100,c:str='C',f:str='F',**kw:Any):
    return l.apply(evt('grant','grant-'+id,at,c,f,grant_id=id,amount=amount,priority=priority,expires=expires,**kw))

def use(l:Ledger,amount:int,id:str='u',at:int=1,c:str='C',f:str='F',source:str='test'):
    return l.apply(evt('consume',id,at,c,f,source,amount=amount))

def expect_error(fn:Callable[[],Any],typ:type[BaseException]=ValueError)->str:
    try:fn()
    except typ:return typ.__name__
    raise AssertionError('Expected exception was not raised')

def check(id:str,group:str,label:str,expected:Any,fn:Callable[[],Any],basis:str='Research test design'):
    try:
        actual=fn(); passed=actual==expected
        error=None
    except Exception as ex:
        actual=None;passed=False;error=f'{type(ex).__name__}: {ex}'
    records.append(dict(id=id,group=group,label=label,expected=expected,actual=actual,passed=passed,basis=basis,error=error))

G='OpenMeter Grant + engine/grant.go, fixed commit in sources.json'
R='OpenMeter Grant rollover + credit/grant/grant.go'
I='Research design: durable source/id dedup + SQLite transaction'

def t01():
    l=Ledger();grant(l,'monthly',10000,5,expires=1000,rollover_min=10000,rollover_max=10000)
    grant(l,'extra',100000,10,expires=1000,rollover_max=100000)
    a=use(l,15000);s=l.snapshot('C','F',1)
    return {'allocations':a['allocations'],'balance':s['balance'],'overage':s['overage']}
check('D01','domain','Priority: monthly allowance before extra grant',{'allocations':{'monthly':10000,'extra':5000},'balance':95000,'overage':0},t01,G)

def t02():
    l=Ledger();grant(l,'later',100,1,expires=100);grant(l,'earlier',100,1,expires=10)
    return use(l,50)['allocations']
check('D02','domain','Equal priority: earliest expiration',{'earlier':50},t02,G)

def t03():
    l=Ledger();grant(l,'created-first',100);grant(l,'created-last',100)
    return use(l,50)['allocations']
check('D03','domain','Equal priority and expiration: creation order',{'created-first':50},t03,G)

def t04():
    l=Ledger();grant(l,'future',100,effective=10)
    return [l.balance('C','F',9),l.balance('C','F',10)]
check('D04','domain','Effective-time boundary',[0,100],t04,G)

def t05():
    l=Ledger();grant(l,'g',100,expires=10);use(l,30)
    return [l.balance('C','F',9),l.balance('C','F',10)]
check('D05','domain','Expiration boundary after partial use',[70,0],t05,G)

def t06():
    l=Ledger();grant(l);use(l,30);l.apply(evt('void','v',2,grant_id='g'))
    return [l.balance('C','F',2),l.check_projection()]
check('D06','domain','Void: no remaining entitlement, journal reconciles',[0,True],t06,G)

def t07():
    l=Ledger();grant(l,'a',100,c='A');grant(l,'b',200,c='B');use(l,30,c='A')
    return [l.balance('A','F',1),l.balance('B','F',1)]
check('D07','domain','Customer isolation',[70,200],t07)

def t08():
    l=Ledger();grant(l,'a',100,f='A');grant(l,'b',200,f='B');use(l,30,f='A')
    return [l.balance('C','A',1),l.balance('C','B',1)]
check('D08','domain','Feature isolation',[70,200],t08)

def t09():
    l=Ledger();grant(l);use(l,0)
    return l.snapshot('C','F',1)
check('D09','domain','Zero usage is a neutral event',{'balance':100,'usage':0,'overage':0,'grants':{'g':100}},t09)

def t10():
    l=Ledger();grant(l);use(l,130)
    return l.snapshot('C','F',1)
check('D10','domain','Overage is recorded, not hidden or rejected',{'balance':0,'usage':130,'overage':30,'grants':{'g':0}},t10,'OpenMeter Entitlement (balance/usage/overage)')

def reset_scenario(b:int,lo:int,hi:int)->int:
    l=Ledger();grant(l,amount=b,rollover_min=lo,rollover_max=hi)
    l.apply(evt('reset','r',1));return l.balance('C','F',1)
check('P01','period','Rollover: restore fixed allowance',10000,lambda:reset_scenario(0,10000,10000),R)
check('P02','period','Rollover: cap remaining amount',50,lambda:reset_scenario(70,0,50),R)
check('P03','period','Rollover: guaranteed minimum',50,lambda:reset_scenario(30,50,100),R)

def t14():
    l=Ledger();grant(l,expires=10,rollover_min=100,rollover_max=100)
    l.apply(evt('reset','r',10));return l.balance('C','F',10)
check('P04','period','Reset must not revive an expired grant',0,t14,R)

def t15():
    l=Ledger();grant(l,at=10,rollover_min=0,rollover_max=0)
    l.apply(evt('reset','r',10));return l.balance('C','F',10)
check('P05','period','Grant effective at reset belongs to new period',100,t15,'OpenMeter Grant caveats')

def t16():
    l=Ledger();grant(l);l.apply(evt('reset','r1',10))
    return expect_error(lambda:l.apply(evt('reset','r2',10)))
check('P06','period','Second reset in same minute rejected','ValueError',t16,'OpenMeter Entitlement reset frequency')

def over_reset(preserve:bool):
    l=Ledger();grant(l,rollover_min=100,rollover_max=100);use(l,130)
    l.apply(evt('reset','r',10,preserve_overage=preserve))
    return l.snapshot('C','F',10)
check('P07','period','Preserve overage at reset',{'balance':70,'usage':0,'overage':0,'grants':{'g':70}},lambda:over_reset(True),'OpenMeter Entitlement + engine/reset.go')
check('P08','period','Forgive overage at reset',{'balance':100,'usage':0,'overage':0,'grants':{'g':100}},lambda:over_reset(False),'OpenMeter Entitlement + engine/reset.go')

def t19():
    l=Ledger();grant(l,amount=10000)
    results=[use(l,700,id='same') for _ in range(5)]
    return {'balance':l.balance('C','F',1),'usage':l.snapshot('C','F',1)['usage'],
            'duplicates':sum(r['duplicate'] for r in results),'committed_usage_events':len(l.event_log())-1}
check('T01','transaction','Five deliveries of one event, one debit',{'balance':9300,'usage':700,'duplicates':4,'committed_usage_events':1},t19,I)

def t20():
    l=Ledger();grant(l);use(l,30)
    error=expect_error(lambda:use(l,31))
    return [error,l.balance('C','F',1)]
check('T02','transaction','Identity collision with changed payload rejected',['ValueError',70],t20,I)

def t21():
    l=Ledger();grant(l);use(l,30,id='same',source='A');use(l,20,id='same',source='B')
    return l.balance('C','F',1)
check('T03','transaction','Same id from distinct sources is not duplicate',50,t21,I)

def t22():
    l=Ledger();grant(l,amount=10000);e=evt('consume','fault',1,amount=700)
    error=expect_error(lambda:l.apply(e,fail_after_projection=True),RuntimeError)
    before=[l.balance('C','F',1),len(l.event_log()),len(l.journal())]
    l.apply(e)
    return {'error':error,'after_failure':before,'after_retry':l.balance('C','F',1),'reconciled':l.check_projection()}
check('T04','transaction','Exception between projection update and commit',{'error':'RuntimeError','after_failure':[10000,1,1],'after_retry':9300,'reconciled':True},t22,I)

def t23():
    with tempfile.TemporaryDirectory() as td:
        p=Path(td)/'db.sqlite';l=Ledger(p);grant(l);use(l,30);l.close()
        n=Ledger(p);out=n.snapshot('C','F',1);n.close();return out
check('T05','transaction','Reopen database without loss of committed state',{'balance':70,'usage':30,'overage':0,'grants':{'g':70}},t23,I)

def t24():
    l=Ledger();grant(l,'monthly',10000,5,expires=1000,rollover_min=10000,rollover_max=10000)
    grant(l,'extra',100000,10,expires=1000,rollover_max=100000)
    use(l,15000);l.apply(evt('reset','r',10));use(l,2000,id='u2',at=11)
    replica=Ledger()
    for e in l.event_log():replica.apply(e)
    return {'same_state':l.snapshot('C','F',11)==replica.snapshot('C','F',11),
            'reconciled':l.check_projection() and replica.check_projection(),
            'balance':replica.balance('C','F',11),'events':len(l.event_log())}
check('T06','transaction','Replay committed events and reconcile entries',{'same_state':True,'reconciled':True,'balance':103000,'events':5},t24,I)

def t25():
    with tempfile.TemporaryDirectory() as td:
        p=Path(td)/'concurrent.sqlite';l=Ledger(p);l.db.execute('PRAGMA journal_mode=WAL');grant(l,amount=10000)
        calls=[(i,j) for i in range(100) for j in range(4)]
        def execute(pair):
            x=Ledger(p)
            try:return use(x,10,id=f'usage-{pair[0]}')['duplicate']
            finally:x.close()
        with ThreadPoolExecutor(max_workers=8) as pool: duplicate=list(pool.map(execute,calls))
        out={'calls':len(calls),'duplicate':sum(duplicate),'committed':len(l.event_log())-1,
             'balance':l.balance('C','F',1),'usage':l.snapshot('C','F',1)['usage'],'reconciled':l.check_projection()}
        l.close();return out
check('T07','transaction','Eight local workers: 400 calls, 100 unique events',{'calls':400,'duplicate':300,'committed':100,'balance':9000,'usage':1000,'reconciled':True},t25,I)

def t26():
    a=Ledger();b=Ledger();grant(a,expires=10);grant(b,expires=20)
    before=[a.balance('C','F',0),b.balance('C','F',0)]
    after=[a.balance('C','F',15),b.balance('C','F',15)]
    return {'equal_scalar_before':before[0]==before[1],'valid_balances_after':after,
            'naive_counter_after':[100,100]}
check('C01','counterexample','Identical aggregate balances hide different expiration states',{'equal_scalar_before':True,'valid_balances_after':[0,100],'naive_counter_after':[100,100]},t26,'Original information-loss counterexample, based on grant expiration')

def t27():
    l=Ledger();grant(l)
    error=expect_error(lambda:use(l,-1));return [error,l.balance('C','F',1)]
check('V01','validation','Negative usage rejected without side effects',['ValueError',100],t27)

def t28():
    l=Ledger();grant(l);use(l,10,at=10)
    return expect_error(lambda:use(l,20,id='late',at=9))
check('V02','validation','Late event explicitly rejected outside prototype scope','ValueError',t28)

# Exhaustive finite domain, not a statistical sample and not synthetic business data.
values=[0,1,5,10,100]
k=0
for balance in values:
    for lower in values:
        for upper in values:
            if lower>upper:continue
            k+=1
            expected=lower if balance<lower else upper if balance>upper else balance
            check(f'B{k:03}','boundary','rollover b=%d min=%d max=%d'%(balance,lower,upper),expected,
                  lambda b=balance,a=lower,z=upper:reset_scenario(b,a,z),R)

summary={'executed_utc':datetime.now(timezone.utc).isoformat(),
         'python':platform.python_version(),'sqlite':sqlite3.sqlite_version,'platform':platform.platform(),
         'directed_checks':sum(r['group']!='boundary' for r in records),
         'boundary_checks':sum(r['group']=='boundary' for r in records),
         'total_checks':len(records),'passed':sum(r['passed'] for r in records),
         'failed':sum(not r['passed'] for r in records),
         'seed':None,'inputs':'Deterministic test fixtures; no production measurements',
         'openmeter_executed':False,'network_calls':False,
         'hashes':{n:hashlib.sha256((Path(__file__).parent/n).read_bytes()).hexdigest()
                   for n in ['ledger.py','run_tests.py']}}
(OUT/'results.json').write_text(json.dumps({'summary':summary,'checks':records},indent=2),encoding='utf-8')
with (OUT/'results.csv').open('w',newline='',encoding='utf-8') as f:
    writer=csv.DictWriter(f,fieldnames=['id','group','label','expected','actual','passed','basis','error']);writer.writeheader()
    for r in records:
        row=dict(r)
        for field in ('expected','actual'):row[field]=json.dumps(row[field],sort_keys=True)
        writer.writerow(row)
print(json.dumps(summary,indent=2))
for r in records:
    if not r['passed']:print('FAILED',r)
if summary['failed']:sys.exit(1)
