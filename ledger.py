"""Independent, deliberately limited credit-grant demonstrator for the TCC.

Not OpenMeter code, not an SDK, not a production billing system.
Public domain rules: OpenMeter grant priority, eligibility, void and rollover.
Research design choices: integer units, chronological minute clock, SQLite
atomic projection + journal, durable (source,id) idempotency, no network.
Unsupported: late event recomputation, recurrence, tax, payment, distributed
consensus, authorization and multi-region operation.
"""
from __future__ import annotations
import json
import sqlite3
from pathlib import Path
from typing import Any

SCHEMA = """
CREATE TABLE IF NOT EXISTS state (
 customer TEXT NOT NULL, feature TEXT NOT NULL,
 clock INTEGER NOT NULL DEFAULT -1, last_reset INTEGER NOT NULL DEFAULT -1,
 usage INTEGER NOT NULL DEFAULT 0 CHECK(usage>=0),
 overage INTEGER NOT NULL DEFAULT 0 CHECK(overage>=0),
 PRIMARY KEY(customer,feature));
CREATE TABLE IF NOT EXISTS grants (
 customer TEXT NOT NULL, feature TEXT NOT NULL, id TEXT NOT NULL,
 amount INTEGER NOT NULL CHECK(amount>=0), remaining INTEGER NOT NULL CHECK(remaining>=0),
 priority INTEGER NOT NULL CHECK(priority BETWEEN 0 AND 255),
 effective INTEGER NOT NULL, expires INTEGER NOT NULL, created INTEGER NOT NULL,
 rollover_min INTEGER NOT NULL CHECK(rollover_min>=0),
 rollover_max INTEGER NOT NULL CHECK(rollover_max>=rollover_min),
 voided INTEGER, PRIMARY KEY(customer,feature,id));
CREATE TABLE IF NOT EXISTS events (
 seq INTEGER PRIMARY KEY AUTOINCREMENT, source TEXT NOT NULL, id TEXT NOT NULL,
 customer TEXT NOT NULL, feature TEXT NOT NULL, at INTEGER NOT NULL,
 kind TEXT NOT NULL, payload TEXT NOT NULL, result TEXT,
 UNIQUE(source,id));
CREATE TABLE IF NOT EXISTS entries (
 seq INTEGER PRIMARY KEY AUTOINCREMENT, event_seq INTEGER NOT NULL REFERENCES events(seq),
 customer TEXT NOT NULL, feature TEXT NOT NULL, grant_id TEXT,
 kind TEXT NOT NULL, delta INTEGER NOT NULL);
"""

class Ledger:
    def __init__(self, path: str | Path = ':memory:') -> None:
        self.path = str(path)
        self.db = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.execute('PRAGMA foreign_keys=ON')
        self.db.execute('PRAGMA busy_timeout=30000')
        self.db.executescript(SCHEMA)

    def close(self) -> None:
        self.db.close()

    def _active(self, c: str, f: str, at: int) -> list[sqlite3.Row]:
        return list(self.db.execute('''SELECT * FROM grants WHERE customer=? AND feature=?
          AND effective<=? AND expires>? AND (voided IS NULL OR voided>?)
          ORDER BY priority,expires,created,id''', (c,f,at,at,at)))

    def balance(self, c: str, f: str, at: int) -> int:
        state = self.db.execute('SELECT clock FROM state WHERE customer=? AND feature=?',(c,f)).fetchone()
        if state and at < state['clock']:
            raise ValueError('Historical queries require journal replay in this demonstrator')
        return sum(g['remaining'] for g in self._active(c,f,at))

    def snapshot(self, c: str, f: str, at: int) -> dict[str, Any]:
        s = self.db.execute('SELECT * FROM state WHERE customer=? AND feature=?',(c,f)).fetchone()
        return {'balance':self.balance(c,f,at), 'usage':s['usage'] if s else 0,
                'overage':s['overage'] if s else 0,
                'grants':{g['id']:g['remaining'] for g in self._active(c,f,at)}}

    def _entry(self, seq:int,c:str,f:str,gid:str|None,kind:str,delta:int)->None:
        self.db.execute('INSERT INTO entries(event_seq,customer,feature,grant_id,kind,delta) VALUES(?,?,?,?,?,?)',
                        (seq,c,f,gid,kind,delta))

    def _burn(self,c:str,f:str,at:int,amount:int,seq:int,kind:str='consume')->tuple[dict[str,int],int]:
        left=amount
        allocations={}
        for g in self._active(c,f,at):
            take=min(left,g['remaining'])
            if take:
                self.db.execute('UPDATE grants SET remaining=remaining-? WHERE customer=? AND feature=? AND id=?',
                                (take,c,f,g['id']))
                self._entry(seq,c,f,g['id'],kind,-take)
                allocations[g['id']]=take
                left-=take
            if left==0: break
        return allocations,left

    def apply(self, event: dict[str,Any], *, fail_after_projection: bool=False)->dict[str,Any]:
        """Atomically persist event, grant projection and journal entries.

        The fault switch injects an exception before commit, not a power failure.
        Same identity with different payload is a conflict, never silent success.
        """
        e=dict(event)
        c,f,at,kind=(e[k] for k in ('customer','feature','at','kind'))
        if not isinstance(at,int) or at<0: raise ValueError('at must be a nonnegative minute index')
        if not e.get('source') or not e.get('id'): raise ValueError('source and id required')
        payload=json.dumps(e,sort_keys=True,separators=(',',':'))
        self.db.execute('BEGIN IMMEDIATE')
        try:
            old=self.db.execute('SELECT payload,result FROM events WHERE source=? AND id=?',
                                (e['source'],e['id'])).fetchone()
            if old:
                if old['payload']!=payload: raise ValueError('Identity conflict: payload changed')
                self.db.execute('COMMIT')
                return {'duplicate':True,**json.loads(old['result'])}
            self.db.execute('INSERT OR IGNORE INTO state(customer,feature) VALUES(?,?)',(c,f))
            s=self.db.execute('SELECT * FROM state WHERE customer=? AND feature=?',(c,f)).fetchone()
            if at<s['clock']: raise ValueError('Late event outside supported chronological scope')
            cur=self.db.execute('INSERT INTO events(source,id,customer,feature,at,kind,payload) VALUES(?,?,?,?,?,?,?)',
                                (e['source'],e['id'],c,f,at,kind,payload))
            seq=cur.lastrowid
            result:dict[str,Any]={'allocations':{},'overage':0}
            if kind=='grant':
                amount=e['amount']; eff=e.get('effective',at); exp=e['expires']
                mi=e.get('rollover_min',0); ma=e.get('rollover_max',0)
                if not all(isinstance(x,int) for x in (amount,eff,exp,mi,ma,e['priority'])):
                    raise ValueError('Integer units and times required')
                if amount<0 or exp<=eff or eff<at: raise ValueError('Invalid grant or past grant unsupported')
                self.db.execute('''INSERT INTO grants(customer,feature,id,amount,remaining,priority,effective,
                                expires,created,rollover_min,rollover_max) VALUES(?,?,?,?,?,?,?,?,?,?,?)''',
                                (c,f,e['grant_id'],amount,amount,e['priority'],eff,exp,seq,mi,ma))
                self._entry(seq,c,f,e['grant_id'],'grant',amount)
            elif kind=='consume':
                amount=e['amount']
                if not isinstance(amount,int) or amount<0: raise ValueError('Invalid usage')
                alloc,over=self._burn(c,f,at,amount,seq)
                self.db.execute('UPDATE state SET usage=usage+?,overage=overage+? WHERE customer=? AND feature=?',
                                (amount,over,c,f))
                if over:self._entry(seq,c,f,None,'overage',over)
                result={'allocations':alloc,'overage':over}
            elif kind=='void':
                g=self.db.execute('SELECT * FROM grants WHERE customer=? AND feature=? AND id=?',
                                  (c,f,e['grant_id'])).fetchone()
                if not g: raise ValueError('Unknown grant')
                if g['voided'] is not None: raise ValueError('Already voided')
                self.db.execute('UPDATE grants SET voided=?,remaining=0 WHERE customer=? AND feature=? AND id=?',
                                (at,c,f,g['id']))
                self._entry(seq,c,f,g['id'],'void',-g['remaining'])
            elif kind=='reset':
                if at<=s['last_reset']: raise ValueError('Reset must be after last reset minute')
                for g in self._active(c,f,at):
                    if g['effective']==at: continue  # Belongs to the new period.
                    new=min(g['rollover_max'],max(g['remaining'],g['rollover_min']))
                    self.db.execute('UPDATE grants SET remaining=? WHERE customer=? AND feature=? AND id=?',
                                    (new,c,f,g['id']))
                    self._entry(seq,c,f,g['id'],'rollover',new-g['remaining'])
                over=s['overage'] if e.get('preserve_overage',False) else 0
                alloc,over=self._burn(c,f,at,over,seq,'preserved_overage')
                self.db.execute('UPDATE state SET last_reset=?,usage=0,overage=? WHERE customer=? AND feature=?',
                                (at,over,c,f))
                result={'allocations':alloc,'overage':over}
            else: raise ValueError('Unsupported event kind')
            self.db.execute('UPDATE state SET clock=? WHERE customer=? AND feature=?',(at,c,f))
            self.db.execute('UPDATE events SET result=? WHERE seq=?',(json.dumps(result,sort_keys=True),seq))
            if fail_after_projection: raise RuntimeError('Injected fault after projection, before commit')
            self.db.execute('COMMIT')
            return {'duplicate':False,**result}
        except BaseException:
            if self.db.in_transaction:self.db.execute('ROLLBACK')
            raise

    def journal(self)->list[dict[str,Any]]:
        return [dict(r) for r in self.db.execute('SELECT * FROM entries ORDER BY seq')]

    def event_log(self)->list[dict[str,Any]]:
        return [json.loads(r['payload']) for r in self.db.execute('SELECT payload FROM events ORDER BY seq')]

    def check_projection(self)->bool:
        rows=self.db.execute('''SELECT g.customer,g.feature,g.id,g.remaining,COALESCE(SUM(e.delta),0) replayed
          FROM grants g LEFT JOIN entries e ON g.customer=e.customer AND g.feature=e.feature AND g.id=e.grant_id
          GROUP BY g.customer,g.feature,g.id''')
        return all(r['remaining']==r['replayed'] for r in rows)
