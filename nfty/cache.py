import nfty.db as db
import nfty.njson as json

def user_cache(id, meta=None):
    if not meta:
        PSQL = '''SELECT meta, last_updated FROM users.cache WHERE id = %s'''
        return db.fetchone(PSQL, id)
    else:
        meta = json.jc(meta)
        PSQL = '''INSERT INTO users.cache (id, meta) VALUES (%s, %s) ON CONFLICT (id) DO UPDATE SET meta = %s'''
        return db.execute(PSQL, id, meta, meta)

def sync(route, meta):
    meta = json.jc(meta)
    PSQL = ''' INSERT INTO cache.data VALUES (%s, %s) ON CONFLICT (id) DO UPDATE SET meta = %s '''
    return db.Pcursor().execute(PSQL, route, meta, meta)

def retrieve(route):
    PSQL =  ''' SELECT meta FROM cache.data WHERE id = %s '''
    return db.Pcursor().fetchreturn(PSQL, route)

def log(method, result):
    PSQL = ''' INSERT INTO cache.log (method, result) VALUES (%s, %s) '''
    return db.Pcursor().execute(PSQL, method, result)

def retrieve_log(limit=20):
    PSQL = f''' SELECT method, result, created FROM cache.log ORDER BY created DESC LIMIT {limit}'''
    return db.Pcursor().fetchall(PSQL)

def removed_ad_user(username):
    if username == 'get':
        return db.fetchall(' SELECT "login", removed FROM sdb.removed_ad_users ')
    PSQL = ''' INSERT INTO sdb.removed_ad_users (login) VALUES (%s) '''
    return db.execute(PSQL, username)

def flush_log():
    PSQL = ''' DELETE FROM cache.log '''
    db.Pcursor().execute(PSQL)
    return 'Success'