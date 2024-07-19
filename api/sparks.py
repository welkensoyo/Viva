import nfty.db as db
from nfty.njson import dc, jc, lc

user_table = 'users.user'
spark_table = 'cache.sparks'
snuffed_table = 'cache.snuffed'
qry = {
    "available_sparks": f'''SELECT id, meta FROM {user_table} WHERE meta @> '{{"apps":{{"dumpsterfire":"enabled"}}}}' AND id NOT IN (SELECT snuffid WHERE userid = %s UNION SELECT sparkid WHERE userid = %s) ) ORDER BY lastupdated;''',
    "available_sparks_filtered": f'''SELECT id, meta FROM {user_table} WHERE meta @> '{{"apps":{{"dumpsterfire":"enabled"}}}}' and meta@> %s AND id NOT IN (SELECT snuffid WHERE userid = %s UNION SELECT sparkid WHERE userid = %s) ) ORDER BY lastupdated;''',
    "available_sparks_limit": f'''SELECT id, meta FROM {user_table} WHERE meta @> '{{"apps":{{"dumpsterfire":"enabled"}}}}' AND id NOT IN (SELECT snuffid WHERE userid = %s UNION SELECT sparkid WHERE userid = %s) LIMIT %limit% OFFSET %offset% ORDER BY lastupdated''',
    "available_sparks_limit_filtered": f'''SELECT id, meta FROM {user_table} WHERE meta @> '{{"apps":{{"dumpsterfire":"enabled"}}}}' AND meta@> %s AND id NOT IN (SELECT snuffid WHERE userid = %s UNION SELECT sparkid WHERE userid = %s) LIMIT %limit% OFFSET %offset% ORDER BY lastupdated''',
    "sparked": f'''INSERT INTO {spark_table} (userid, sparkid, status) VALUES (%s, %s, %s) RETURNING id; ''',
    "snuffed": f'''INSERT INTO {snuffed_table} (userid, snuffid, status) VALUES (%s, %s, %s) RETURNING id; '''
}


class Sparks:
    def __init__(self, user):
        self.u = user

    def spark_limit_reached(self, limit=3):
        if 'sparks' not in self.u.meta:
            self.u.meta['sparks'] = []
            self.u.save(meta=self.u.meta)
        if self.u.meta['sparks'] < limit:
            return False
        return True

    def sparks(self):
        if not self.spark_limit_reached():
            return db.fetchone(qry["available_sparks"], self.u.id, self.u.id)
        return False

    def sparks_filter(self, filter_keys=None, limit=10, page=0):
        if self.spark_limit_reached():
            return False
        if filter_keys:
            filter_keys = dc(filter_keys)
            return db.fetchall(qry["available_sparks_limit"].replace('%limit%', limit).replace('%offset%', str(page), ), jc(filter_keys), self.u.id, self.u.id)
        return db.fetchall(qry["available_sparks_limit_filtered"], jc(filter_keys), self.u.id, self.u.id)

    def sparked(self, spark_id, status='FIRST'):
        if not self.spark_limit_reached():
            return db.fetchreturn(qry["sparked"], self.u.id, spark_id, status)
        return False  # reached limit of sparks

    def snuffed(self, snuffed_id, status='OUCH'):
        try:
            self.u.meta['sparks'].remove(snuffed_id)
            self.u.save(meta=self.u.meta)
        except:  # didn't exist
            pass
        return db.fetchreturn(qry["snuffed"], self.u.id, snuffed_id, status)
