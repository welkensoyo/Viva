import logging
from typing import Dict
from gevent import spawn
import nfty.db as db
from nfty.ncrypt import check_session, encrypt, decrypt, create_session
import nfty.njson as j
import nfty.wallet as wallet
import nfty.constants as constants
from nfty.communications import do
from api._config import systemoptions
from nfty.tags import Tag

logger = logging.getLogger("AppLogger")

e_table = systemoptions["events"]
enroll_table = systemoptions["enroll"]

qry = {
    "mid": f"""
    INSERT INTO {constants.ENTITY_TABLE}  (id, meta, zipcode) 
    VALUES (%s, %s, %s)
    ON CONFLICT (id) DO UPDATE SET meta = client.meta || %s  RETURNING id 
""",
    "update": f"""UPDATE {constants.ENTITY_TABLE} SET meta = meta || %s  WHERE id = %s; """,
    "nfty": f""" 
    SELECT id, meta-'enroll'-'tokens'-'kyc', hashcode 
    FROM {constants.ENTITY_TABLE} 
    WHERE id=%s or meta @> %s::jsonb or meta @> %s::jsonb 
""",
    "name": f""" 
    SELECT id, meta-'enroll', hashcode 
    FROM {constants.ENTITY_TABLE} 
    WHERE meta @> %s::jsonb or meta @> %s::jsonb 
""",
    "iframe": f""" SELECT meta->>'apikey' FROM {constants.ENTITY_TABLE} WHERE meta @> %s::jsonb or meta @> %s::jsonb """,
    "apikey": f""" SELECT meta->>'apikey' FROM {constants.ENTITY_TABLE} WHERE id=%s """,
    "apikeys": f""" SELECT id, meta->>'apikey', meta->>'name' FROM {constants.ENTITY_TABLE} WHERE id in %s """,
    "hashcode": f""" SELECT meta->>'apikey' FROM {constants.ENTITY_TABLE} WHERE hashcode=%s """,
    "link": f""" SELECT meta->>'name', meta->>'timezone' FROM {constants.ENTITY_TABLE} WHERE meta @> %s::jsonb """,
    "linklist": """ SELECT meta->>'name', meta->>'apikey' FROM entity.client """,
    "get": f""" SELECT meta-'enroll' FROM {constants.ENTITY_TABLE} WHERE id = %s """,
    "hierarchy": f""" SELECT id, hashcode, meta->>'name' FROM {constants.ENTITY_TABLE} WHERE id = %s """,
    "setup": f""" SELECT meta->>'name', meta->>'timezone', meta->>'email', meta->>'callback', meta->>'tax', meta->'config' FROM {constants.ENTITY_TABLE} WHERE id::text = %s """,
    "all": f""" SELECT id, zipcode, meta->>'name', meta-'enroll'-'tokens'-'kyc'-'subpricing', hashcode, lastupdated FROM {constants.ENTITY_TABLE} """,
    "enrollment": f"""SELECT meta->'apicreds'->>'token', meta->>'enroll' FROM {constants.ENTITY_TABLE} WHERE id = %s """,
    "timezones": """SELECT * FROM pg_timezone_names()  """,
    "timezone": """ SELECT * FROM pg_timezone_names() WHERE name = %s""",
    "lookup": f""" SELECT id, meta->>'name' FROM {constants.ENTITY_TABLE} WHERE id::text=%s or meta @> %s::jsonb """,
    "users": f""" SELECT id, hashcode, zipcode, meta-'tokens'  FROM entity.customer WHERE meta->'clientid' ? %s  """,
    "admins": """ SELECT id, hashcode, zipcode, meta-'tokens'  FROM entity.customer WHERE meta @> '{"utype":"admin"}' """,
    "children-all": f""" SELECT id, '', meta->>'dba_name', meta-'enroll'-'tokens'-'kyc'-'subpricing', status, lastupdated::text  FROM entity.enroll WHERE meta@>%s 
                        UNION SELECT id, hashcode, meta->>'name', meta-'enroll'-'tokens'-'kyc'-'subpricing'-'whitelabel', 'LIVE', lastupdated::text FROM entity.client WHERE meta@>%s OR id::text = ANY(%s) ORDER BY 3 """,
    "children": f""" SELECT id, hashcode, meta->>'name', meta-'enroll'-'tokens'-'kyc'-'subpricing', 'LIVE', lastupdated::text FROM entity.client WHERE meta@>%s OR id::text = ANY(%s) ORDER BY 3   """,
    "children-list": f""" SELECT ARRAY(SELECT id FROM {constants.ENTITY_TABLE} WHERE meta @> %s::jsonb or id::text = ANY(%s))::text[] """,
    "parents": f""" SELECT meta->>'name', id FROM {constants.ENTITY_TABLE} WHERE meta@>'{{"level":"Parent"}}' ORDER BY 1 """,
    "clients": f""" SELECT id, hashcode, meta->'parent'->>'name', meta->>'name', meta->'billing'->>'legal_name', meta->'config'->>'enabled', lastupdated::text FROM entity.client WHERE meta@>%s OR id::text = ANY(%s) ORDER BY 3   """,
    "clients-meta": f""" SELECT meta-'enroll'-'tokens'-'kyc'-'subpricing'-'whitelabel'-'child'-'qa'-'wallet'-'confirmation'-'acceptedCards'-'expressSubAccount'-'fee', id, hashcode, lastupdated FROM entity.client  """,
    "enrolled": f""" INSERT INTO {e_table} (id, clientid, meta, state, created) VALUES (%s, %s, %s, %s, now()) """,
    "not_enrolled": f""" INSERT INTO {e_table} (id, clientid, meta, state, created) VALUES (%s, %s, %s, %s, now())
""",
}


def search(id, hash=False, name_search=True):
    id = str(id).strip()
    logger.debug(f"searching for id: {id}")
    name = ""
    hashcode = ""
    default = constants.CLIENT_ID_DEFAULT_API
    if not id or id == constants.HASH_CODE_TRIPLE:
        id = default
    if j.checkuuid(id):
        PSQL = (
            f""" SELECT id, meta->>'name', hashcode FROM {constants.ENTITY_TABLE} WHERE id=%s """
        )
        id, name, hashcode = db.fetchone(PSQL, id) or (id, "", None)
    elif len(id) == 6:
        PSQL = f""" SELECT id, meta->>'name', hashcode FROM {constants.ENTITY_TABLE} WHERE hashcode=%s or meta @> %s::jsonb """
        id, name, hashcode = db.fetchone(PSQL, id, f'{{"name":"{id}"}}') or (
            id,
            "",
            None,
        )
    if not name and name_search:
        PSQL = f""" SELECT id, meta->>'name', hashcode FROM {constants.ENTITY_TABLE} WHERE meta @> %s::jsonb or meta @> %s::jsonb or meta @> %s::jsonb """
        id, name, hashcode = db.fetchone(
            PSQL, f'{{"curpayid":"{id}"}}', f'{{"name":"{id}"}}', f'{{"mid":"{id}"}}'
        ) or (id, "", None)
    elif not name and not name_search:
        PSQL = f""" SELECT id, meta->>'name', hashcode FROM {constants.ENTITY_TABLE} WHERE meta @> %s::jsonb or meta @> %s::jsonb or meta @> %s::jsonb """
        id, name, hashcode = db.fetchone(
            PSQL, f'{{"curpayid":"{id}"}}', f'{{"primary":"{id}"}}', f'{{"mid":"{id}"}}'
        ) or (id, "", None)
    if not name:
        PSQL = (
            f""" SELECT id, meta->>'name', hashcode FROM {constants.ENTITY_TABLE} WHERE id=%s """
        )
        id, name, hashcode = db.fetchone(PSQL, default)
    if hash:
        return id, name, hashcode or hash
    return id, name


class Client:
    def __init__(self, id):
        if isinstance(id, list):
            id = id[0]
        self.id = str(id)
        self.apikey = ""
        self.name = ""
        self.tags = {}
        self.tz = "US/Central"
        self.contact = {}
        self.keystore = {}
        self.callback = ""
        self.enabled = True
        if len(self.id) == 6:
            id = db.fetchreturn(qry["hashcode"], self.id)
            if id:
                self.id = id
        if j.checkuuid(self.id):
            self.id, meta, self.hashcode = db.fetchone(qry["nfty"],self.id) or (self.id, {})
            self.__dict__.update(meta)

        else:
            sid = self.session_check(self.id)
            if sid:
                self.id = sid
            else:  # search for name
                self.id, meta, self.hashcode = db.fetchone(qry["name"], j.jc({"legal_name": self.id}), j.jc({"name": self.id}) ) or (self.id, {})
                self.__dict__.update(meta)

    @staticmethod
    def session(key):
        if key == 'login':
            return key
        return check_session(key)


    def dicted(self):
        return {k: v for k, v in self.__dict__.items() if k not in self.IMMUTABLE}

    def metadata(self) -> Dict:
        """
        Returns a combination of enrollment data and other metadata

        Returns:
            Dict
        """
        base = {k: v for k, v in self.__dict__.items()}
        enrollment = db.fetchreturn(qry["raw_enrollment"], self.name)
        logger.info(
            f"This enrollment entry pulled out of the database for metadata: {enrollment}"
        )
        if not enrollment:
            logger.error(f"Enrollment data not found on client: {base}")
        else:
            for key, value in enrollment.items():
                if key not in base.keys():
                    base[key] = value

        return base

    def details(self):
        return dict(
            zip(
                (
                    "hashcode",
                    "name",
                    "timezone",
                    "billing"
                ),
                (
                    self.hashcode,
                    self.name,
                    self.tz,
                    self.contact
                ),
            )
        )

    def enroll(self, meta: Dict = None) -> Dict:
        """
        This just writes and returns an enrollment entry to the database.

        I believe this is also the LAST step in our state management of client onboardings.
        """
        logger.info(f"Enroll called with meta: {meta}")
        if not meta:
            x = db.fetchone(qry["enrollment"], self.id)
            logger.info(f"Enrollment query result: {x}")
            if not x:
                logger.error(f"Could not find client with id: {self.id}")
                return {}
            apicreds, enrollment = x
            try:
                result = decrypt(enrollment, self.id) or {}
                logger.info(f"Decrypted enrollment result: {result}")
                return result
            except Exception as e:
                logger.exception("Exception enrolling clients")
                return decrypt(enrollment, apicreds) or {}
        else:
            db.execute(qry["update"], j.jc({"enroll": encrypt(meta, self.id)}), self.id)
        return meta or {}

    def update_enrollment(self):
        meta = self.enroll()
        db.execute(qry["move_enrollment"], self.id, meta)
        return meta

    def save(self, meta):
        # meta = wallet.Wallet(self.id, client=True).achupdate(meta)
        if "apikey" in meta:
            if not meta["apikey"] or meta["apikey"] == 'login':
                meta.pop("apikey", "")
        m = j.jc(meta)
        self.id = db.fetchreturn(qry["mid"], self.id, m, self.zipcode or 0, m)
        return self.get()

    def get(self):
        if j.checkuuid(self.id):
            meta = db.fetchreturn(qry["get"], self.id) or {}
            self.__dict__.update(meta)
            return self.dicted()
        return {}

    def setup(self, meta=None):
        meta.pop("ip", "")
        if meta:
            meta = wallet.Wallet(self.id, client=True).achupdate(meta)
            keys = ("timezone", "email", "callback", "logo")
            self.save({k: meta[k] for k in keys if meta.get(k)})
        return self.details()

    def ach(self, token=False):
        return wallet.Wallet(self.id, client=True)

    def timezone(self, tz):
        if tz:
            if db.fetchone(qry["timezone"], tz):
                self.save({"timezone": tz})
                return True
            return False
        else:
            return db.fetchall(qry["timezones"])

    def children(self, mode="ALL", user_clients=None):
        mode = mode.upper()
        ids = [self.id]
        ids.extend(j.lc(user_clients) or [])
        if mode == "CLIENTS":
            return db.fetchall( qry["clients"], f'{{"parent":{{"id":"{self.id}"}}}}', (ids,))
        if mode == "ALLCLIENTS":
            return db.fetchall(qry["clients-tpp"])
        if mode == "CHILDREN":
            return db.fetchall( qry["children"], f'{{"parent":{{"id":"{self.id}"}}}}', (ids,))
        if mode == "ALL":
            meta = f'{{"parent":{{"id":"{self.id}"}}}}'
            return db.fetchall(qry["children-all"], meta, meta, (ids,))
        if mode == "META":
            return db.fetchall( qry["clients-meta"], f'{{"parent":{{"id":"{self.id}"}}}}', (ids,))
        else:
            return db.fetchreturn( qry["children-list"], f'{{"parent":{{"id":"{self.id}"}}}}', (ids,))

    def users(self, entityid):
        return db.fetchall(qry["users"], entityid or self.id)

    def admins(self):
        return db.fetchall(qry["admins"])

    def call(self, key):
        if key in qry.keys():
            return db.fetchall(qry[key])
        return {}

    def note(self, meta=None, tag="log"):
        from nfty.notes import Notes
        from api.users import User

        user = User()
        if not meta:
            return Notes.get(self.id)
        else:
            if isinstance(meta, dict):
                return Notes.new(self.id, user.email, meta["tag"], meta["text"])
            return Notes.new(self.id, user.email, tag, meta)

    def session_create(self, expires):
        return create_session(self.apikey, expires)

    def session_check(self, key):
        return check_session(key)

    def respond(self, message):
        spawn(do, self.callback, message)

    def update_bank_info(self):
        meta = self.ach()
        if not "customer_name" in meta:
            wallet.Wallet(self.id, client=True).achupdate(meta, reset=True)
            return self.ach()
        return meta

    def get_tags(self):
        return Tag.client(self)

    def hierarchy(self, id: str) -> Dict:
        parent = db.fetchone(qry["hierarchy"], id)
        if parent:
            self.parent = {"id": parent[0], "hashcode": parent[1], "name": parent[2]}
            self.save({"parent": self.parent})
            return self.parent
        else:
            return {}

    @staticmethod
    def parents():
        return db.fetchall(qry["parents"])

    @staticmethod
    def get_apikey(id):
        if id:
            return db.fetchreturn(qry["apikey"], id)
        return ""

    @staticmethod
    def all():
        return db.fetchall(qry["all"])

def get_all_clients():
    PSQL = """ SELECT id, meta->>'name' FROM entity.client ORDER BY 2 """
    return db.fetchall(PSQL)


def get_name(clientid):
    PSQL = "SELECT meta->>'name' FROM entity.client WHERE id=%s "
    return db.fetchreturn(PSQL, clientid)
