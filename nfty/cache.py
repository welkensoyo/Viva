# -*- coding: utf-8 -*-
import traceback
import logging
import redis
import redis.connection
from gevent import sleep as gsleep
from gevent.queue import LifoQueue
import nfty.db as db
import nfty.njson as json
from api._config import systemoptions


logger = logging.getLogger("AppLogger")

# from redis import Redis
# from rq import Queue

# queue = Queue(connection=Redis())


tokenstring = db.tokenstring
jsoncheck = json.jc
dictcheck = json.dc
redisconfig = systemoptions["redisconfig"]
transtable = systemoptions["cachetable"]
sleepytime = 5
noticewaittime = 60


class CacheError(Exception):
    pass


def fetch(key):
    return RedisCache().get(key)


def put(key, meta):
    return RedisCache().set(key, meta)


def event_get(id):
    return db.fetchreturn(id)


# REDIS TOOLS
_rconn = None


def redisconn():
    global _rconn
    if _rconn is None:
        try:
            _rconn = redis.BlockingConnectionPool(
                host=redisconfig["host"],
                port=redisconfig["port"],
                db=redisconfig["db"],
                max_connections=1000,
                timeout=1,
                queue_class=LifoQueue,
                decode_responses=True,
            )
        except:
            traceback.print_exc()
            _rconn = None
    return _rconn


class RedisCache:
    def __init__(self):
        # in case of a lost connection lets sit and wait till it's online
        global _rconn
        if not _rconn:
            while not _rconn:
                try:
                    redisconn()
                except:
                    print("Attempting Connection To Redis...")
                    gsleep(1)
        self.r = redis.StrictRedis(connection_pool=_rconn)
        self.rpool = self.r.connection_pool

    def get(self, key):
        return self.r.get(key)

    def getonce(self, key):
        pipe = self.r.pipeline()
        x = pipe.get(key).delete(key).execute()[0]
        return x

    def set(self, key, meta, expire=86400, nx=True):
        self.r.set(str(key), jsoncheck(meta), ex=expire, nx=nx)
        return True

    def delete(self, key):
        self.r.delete(key)
        return True

    def append(self, key, meta, expire=86400, nx=True):
        # should probably use this over set
        self.r.append(key, jsoncheck(meta))
        return True

    def scanfor(self, keysearch):
        pipe = self.r.pipeline()
        keylist = []
        for key in self.r.scan_iter(match=keysearch):
            keylist.append(key)
            pipe.get(key)
        values = pipe.execute()
        vallist = list(map(json.loads, values))
        return {k: v for k, v in zip(keylist, vallist)}

    def scanonce(self, keysearch):
        pipe = self.r.pipeline()
        keylist = []
        values = []
        for key in self.r.scan_iter(match=keysearch):
            keylist.append(key)
            values.append(pipe.get(key).delete(key).execute()[0])
        if values:
            vallist = list(map(json.loads, values))
            return {k: v for k, v in zip(keylist, vallist)}
        return {}

    def flush(self):
        self.r.flushall()

    def scanforncrypt(self, clienthash):
        import nfty.ncrypt as ncrypt

        return ncrypt.encrypt(jsoncheck(self.scanfor(clienthash)), clienthash)

    def scandelete(self, keysearch):
        pipe = self.r.pipeline()
        for key in self.r.scan_iter(keysearch):
            pipe.delete(key)
        pipe.execute()
        return

    def pending(self, keysearch):
        # keysearch = "{}-*".format(self.clienthash)
        xdict = {}
        pipe = self.r.pipeline()
        for key in self.r.scan_iter(keysearch):
            meta = pipe.get(key).delete(key).execute()[0]
            xdict[key] = dictcheck(meta)
        self.savedb(xdict)
        return xdict

    def savedb(self, xdict):
        tablename = systemoptions["cachetable"]
        PSQL = """ INSERT INTO {0} (hashcode, meta, status) VALUES (%s, %s, %s)
                       ON CONFLICT (hashcode)
                       DO UPDATE SET meta = {1}.meta || %s, status = {1}.status || %s""".format(
            tablename, tablename.split(".")[2]
        )

        setofqueries = []
        for key, value in list(xdict.items()):
            if not isinstance(key, (str)):
                key = key.decode()
            keys = key.split("-")
            meta = {key: value}
            status = {key: "PENDING"}
            meta["clienthash"] = keys[0]
            meta["userhash"] = keys[1]
            meta["posid"] = keys[2]
            meta = jsoncheck(meta)
            status = jsoncheck(status)
            setofqueries.append((PSQL, (keys[1], meta, status, meta, status)))
        db.spoolq(setofqueries)
        return None

    def connections(self):
        return self.rpool._created_connections

    def inuse(self):
        return self.rpool._in_use_connections


class Notificator(object):
    def __init__(self):
        self.noticeid = "NOTICE:{}"
        self.responseid = "NOTICE:{}:{}"
        self.response_key = "NOTICE:{}:*"
        self.r = RedisCache()

    def get(self, userhash):
        notice = self.noticeid.format(userhash)
        result = self.r.getonce(notice)
        return result

    def get_response(self, userhash):
        notice = self.response_key.format(userhash)
        result = self.r.scanonce(notice)
        return result

    def set(self, userhash, meta, expire=123):
        notice = self.noticeid.format(userhash)
        self.r.set(notice, jsoncheck(meta), expire, True)
        return

    def set_response(self, returnuserhash, fromuserhash, meta, expire=123):
        notice = self.responseid.format(returnuserhash, fromuserhash)
        self.r.set(notice, jsoncheck(meta), expire, True)
        return

    def delete(self, userhash):
        notice = self.noticeid.format(userhash)
        self.r.delete(notice)


def getzip(clienthash):
    PSQL = """ SELECT zipcode FROM entity.client WHERE id = %s """
    return db.Pcursor().fetchone(PSQL, clienthash)[0]
