import logging
from gevent import spawn_later
import nfty.db as db
import nfty.constants as constants
from api._config import systemoptions
from nfty.njson import jc, dc, checkuuid


logger = logging.getLogger("AppLogger")

e_table = systemoptions["events"]
t_table = systemoptions["transaction"]

qry = {
    "id": f"""SELECT id, meta, created FROM {e_table} WHERE id = %s """,
    "get": f"""  with READY as ( 
                    UPDATE cache.events SET state='RETRIEVED' WHERE id IN ( 
                    SELECT id FROM cache.events 
                    WHERE "state" = 'READY' AND (id::text=%s OR meta @> %s)          
                    ORDER BY created FOR UPDATE SKIP LOCKED LIMIT 1
                 ) RETURNING id, meta, created) 
                 SELECT * FROM READY ORDER BY created ASC """,
    "ready": f""" with READY as ( UPDATE {e_table} SET state='RETRIEVED'
            WHERE id IN ( SELECT id FROM {e_table} WHERE "state" = 'READY' and clientid = %s
            ORDER BY created FOR UPDATE SKIP LOCKED) RETURNING id, meta, created)
            SELECT * FROM READY ORDER BY created ASC """,
    "all": f""" SELECT id, meta, created, state FROM {e_table} WHERE clientid = %s  """,
    "new": f""" INSERT INTO {e_table} (clientid, meta) VALUES (%s, %s) RETURNING id""",
    "tranmerge": f""" UPDATE {t_table} SET meta = jsonb_update(meta, %s) WHERE meta @> %s RETURNING meta->>'id' """,
    "tranupdate": f""" UPDATE {t_table} SET meta = meta||%s WHERE meta @> %s RETURNING meta->>'id' """,
    "next": f""" UPDATE {e_table} SET "state"='RETRIEVED' 
            WHERE id IN ( SELECT id FROM {e_table} WHERE "state" = 'READY' and clientid = %s 
            ORDER BY created ASC FOR UPDATE SKIP LOCKED limit 1) RETURNING clientid, meta, created """,
    "flush": f"""DELETE FROM {e_table} WHERE state = 'RETRIEVED' and clientid=%s RETURNING clientid """,
    "clean": f"""DELETE FROM {e_table} WHERE meta@>%s RETURNING id """,
    "status": f"""UPDATE {e_table} SET state = %s WHERE id = %s""",
}


def callback(clientid, option):
    if not option:
        return next(clientid)
    elif option == "ready":
        return ready(clientid)
    elif option == "all":
        return all(clientid)
    elif option in ("delete", "flush", "clean"):
        return flush(clientid)
    else:
        return get(option)


def get(id):
    def checkjson(id):  # check if the object is actually a lookup field value
        if dc(id, only=True):
            return jc(id)
        return '{"":""}'

    return db.fetchone(qry["get"], checkuuid(id), checkjson(id))


def ready(clientid):
    return db.fetchone(qry["ready"], clientid)


def all(clientid):
    return db.fetchall(qry["all"], clientid)


def clean(meta):
    db.fetchall(qry["clean"], jc(meta))


def callbackurl(option, meta):
    meta["_source"] = option
    x = db.fetchreturn(qry["new"], constants.CLIENT_ID_DEFAULT_API, jc(meta))
    process(x, meta)
    return x


def new(clientid, meta):
    if "externaltransactionid" in meta:  # curpay update
        id = meta["externaltransactionid"]
        if "txstatus" in meta:
            txstatus = meta["txstatus"].strip().lower()
            msg = txstatus
            result = meta["iscanceled"]
            status = True
            if result in (True, "true"):
                status = False
                msg = meta["canceledreason"]
                spawn_later(600, clean, {"externaltransactionid": id})
            if txstatus == "completed":
                spawn_later(600, clean, {"externaltransactionid": id})
            coin = meta.get("currencycode", "")
            db.execute(
                qry["tranupdate"],
                jc({"message": f"{coin}: {msg}", "status": status}),
                f'{{"id" : "{id}"}}',
            )
            db.execute(qry["tranmerge"], jc({"details": [meta]}), f'{{"id" : "{id}"}}')
    return db.fetchreturn(qry["new"], clientid, jc(meta))


def next(clientid):
    return db.fetchone(qry["next"], clientid)


def flush(clientid):
    return [each[0] for each in db.fetchall(qry["flush"], clientid)]


def process(id, meta):
    meta = dc(meta)
    if meta["_source"] == constants.PROCESSOR_BLUESNAP:
        if (
            "merchantId" in meta
            and meta.get("ipnId")
            and meta.get("accountCanProcess") == "Y"
        ):
            from nfty.processors.bluesnap import Enroll
            from nfty.clients import Client
            from nfty.communications import alert

            Enroll(Client(meta["merchantName"])).token(meta["merchantId"])
            db.execute(qry["status"], "ENROLLED", id)
            alert(
                f"BLUESNAP {meta['merchantName']} : {meta['merchantId']} ",
                f"BLUESNAP {meta['merchantName']} : {meta['merchantId']} ",
                f"{constants.EMAIL_CSM}, {constants.EMAIL_COO}, {constants.EMAIL_DEV_TEAM}",
            )

def dumpsterfire_sync(message):
    d = dc(message)
    if 'chat' in d:
        yield d['chat']['meta']['receiver']
    elif 'match' in d:
        yield d['match']['meta'].get('matched')
        yield d['match']['meta'].get('matched_with')
    elif 'id' in d:
        yield d['id']
