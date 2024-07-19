# -*- coding: ISO-8859-1 -*-
import traceback
import logging
import bcrypt
import firebase_admin
import phonenumbers
import requests
from firebase_admin import credentials, auth as firebase_auth
from gevent import sleep
import nfty.constants as constants
import nfty.db as db
import nfty.njson as json
import nfty.wallet as wallet
from bottle import request, redirect
from api._config import appconfig, systemoptions
from nfty.ziptime import ZipTime as nz
from geopy.geocoders import Nominatim


logger = logging.getLogger("AppLogger")


try:
    cred = credentials.Certificate(
        "keys/dumpsterfire-app-c8c05f97353e.json"
    )
    firebase_admin.initialize_app(cred)
except:
    cred = credentials.Certificate(
        "../keys/dumpsterfire-app-c8c05f97353e.json"
    )
    firebase_admin.initialize_app(cred)

secretkey = appconfig["secretcookiekey"]
user_table = 'users.user'

qry = {
    "login": f"""SELECT id, zipcode, meta-'loyalty'-'tokens'-'storage', hashcode FROM {user_table} WHERE meta->>'email' = %s """,
    "insert" : f""" INSERT INTO {user_table} (zipcode, meta) VALUES (%s, %s) RETURNING id, hashcode;""",
    "upsert": f"""
    INSERT INTO {user_table} (meta, zipcode) 
    VALUES (%s, %s) 
    ON CONFLICT ((meta->>'email')) 
    DO UPDATE SET meta = customer.meta || %s, zipcode=%s 
    RETURNING id
""",
    "update": f"""
    UPDATE {user_table} SET meta = meta || %s, zipcode=%s  
    WHERE id = %s;
""",
    "delete": f"""
    DELETE FROM {user_table} WHERE id = %s
""",
    "lookup": f""" 
    SELECT id, meta, hashcode FROM {user_table}
    WHERE id=%s or meta @> %s::jsonb
""",
    "admins": f""" 
    SELECT id, hashcode, meta-'wallet'-'tokens' FROM {user_table}
    WHERE meta->>'utype'='admin' 
""",
    "disable": f""" UPDATE {user_table} SET meta = jsonb_set(meta, '{{app,dumpsterfire}}', '"disabled"')::jsonb WHERE id = %s; """,
    "enable": f""" UPDATE {user_table} SET meta = jsonb_set(meta, '{{app,dumpsterfire}}', '"enabled"')::jsonb WHERE id = %s; """

}


# For Admin Access
def check_admin(fn):
    def check_admin(**kwargs):
        user = User()
        if user.status() and user.utype == constants.ADMIN:
            return fn(**kwargs)
        else:
            redirect("/logout")

    return check_admin


class User:
    NEW = ('search', '_id', 'id')

    def __init__(self, _id=None):
        self.id = _id
        self.tags = {}
        self.alias = ""
        self.avatar = ""
        self.email = ""
        self.phone = ""
        self.fuid = ""
        self.zipcode = ''
        self.latitude = ''
        self.longitude = ''
        self.contact = {}
        self.entities = []
        self.origin = constants.DEFAULT_ORIGIN
        self.wallet = []
        self.session = request.environ["beaker.session"]
        self.utype = "user"
        self.meta = {}
        self.apps = {}
        try:
            if not self.id:
                logger.info(f"Session pulled {self.session}")
                self.__dict__.update(self.session)
                self.session_refresh()
            else:
                self.session_refresh()
        except Exception as exc:
            logger.exception("Exception with session refresh")
        # if self.id:
            # self.wallet = wallet.Wallet(self.id).wallet
        if not self.alias:
            self.alias = self.email.split("@")[0]
        if self.zipcode and not (self.latitude and self.longitude):  # get the longitude and latitude of the user
            geolocator = Nominatim(user_agent="geoapiExercises")
            location = geolocator.geocode(self.zipcode, exactly_one=True)
            self.save(longitude=location.latitude, latitude=location.longitude)

    @classmethod
    def search(cls, id):
        if json.checkuuid(id):
            return cls(id)
        email = f'{{"email":"{id}"}}'
        PSQL = f""" SELECT id FROM {user_table} WHERE meta @> %s::jsonb """
        id = db.fetchreturn(PSQL, email)
        return cls(id)

    def add_entity(self, client_id):
        self.entities.append(client_id)
        self.entities = list(set(self.entities))
        return self

    def remove_entity(self, clientid):
        if clientid:
            try:
                self.entities.pop(clientid)
            except Exception as e:
                logger.exception("Entity Not Found")
        return self

    def today(self):
        zipcode = self.zipcode or self.contact.get('zipcode')
        return nz.ziptolocal(None, zipcode)

    def dicted(self):
        x = {k: v for k, v in self.__dict__.items() if k not in ("session",)}
        if self.id:
            return x
        return "User not found."

    def update_meta(self, key, data):
        data = json.dc(data)
        try:
            self.meta[key].update(data)
        except:
            self.meta[key] = data
        self.save(meta=self.meta)
        print(self.meta[key])
        return self.meta[key]

    def obj(self):
        if self.email:
            return [self.id, self.dicted()]
        return [self.id, {}]

    def status(self):
        if self.id:
            return True
        return False

    def set(self, **kwargs):
        k = kwargs.copy()
        for _ in ("wallet", "session"):
            k.pop(_, "")
        self.__dict__.update(k)
        return self

    def save(self, **kwargs):
        if "entities" in kwargs:
            kwargs["entities"] = list(set(kwargs["entities"] + self.entities))
        self.set(**kwargs)
        meta = json.jc(kwargs)
        if self.id and json.checkuuid(self.id):
            db.execute(qry["update"], meta, self.zipcode or 0, self.id)
        else:
            self.id = db.fetchreturn(
                qry["upsert"], meta, self.zipcode or 0, meta, self.zipcode or 0
            )
        return self

    def parseurl(self, url):
        return url.split("/") if url else ()

    def _session(self):
        return self.session

    def session_refresh(self):
        if self.id and len(self.id) == 6:
            PSQL = f"""SELECT id, meta-'tokens', hashcode FROM {user_table} WHERE hashcode = %s"""
            if login := db.fetchone(PSQL, self.id):
                self.set(id=login[0], hash=login[2], **login[1])
        elif self.email:
            email = f"""{{"email":"{self.email}"}}"""
            PSQL = f"""SELECT id, meta-'tokens', hashcode FROM {user_table} WHERE meta @> %s"""
            if login := db.fetchone(PSQL, email):
                self.set(id=login[0], hash=login[2], **login[1])
        elif self.id:
            if "@" in self.id:
                email = f"""{{"email":"{self.id}"}}"""
                PSQL = f"""SELECT id, meta-'tokens', hashcode FROM {user_table} WHERE meta @> %s"""
                if login := db.fetchone(PSQL, email):
                    self.set(id=login[0], hash=login[2], **login[1])
            elif self.zipcode:
                PSQL = f"""SELECT id, meta-'tokens', hashcode FROM {user_table} WHERE id = %s and zipcode = %s"""
                if login := db.fetchone(PSQL, self.id, self.zipcode):
                    self.set(**login[1], hash=login[2])
            else:
                PSQL = f"""SELECT id, meta-'tokens', hashcode FROM {user_table} WHERE id = %s """
                if login := db.fetchone(PSQL, self.id):
                    self.set(**login[1], hash=login[2])
        if self.id:
            self.session['id'] = self.id
            self.session.save()
        return self

    def check_token(self, token):
        try:
            sleep(3)
            return firebase_auth.verify_id_token(token)
        except Exception as e:
            logger.exception("Exception with firebase_auth.verify_id_token")
            traceback.print_exc()
            return False

    def login(self, u):
        # if not "token" in u:
        #     return self.api_login(u)
        f = self.check_token(u.get("token"))
        if not f:
            # if "apikey" in u and "email" in u:
            #     return self.api_login(u)
            return False
        try:
            origin = f["firebase"]["sign_in_provider"]
        except Exception as e:
            return False
        email = f["email"]
        if isinstance(f["email"], list):
            email = f["email"][0].strip()
        meta = {
            "origin": origin,
            "avatar": f.get("picture"),
            "fuid": f["uid"],
            "email_verified": f["email_verified"],
            "name": f.get("name"),
            "zipcode": u.get("zipcode", None),
            "entities": [u.get("entities") or u.get('entity', None)],
            "phone": u.get("phone") or u.get("cell") or None,
        }

        login = db.fetchone(qry['login'], email)
        if not login:
            meta["email"] = email
            meta['apps'] = {'dumpsterfire':'enabled'}
            id, hash = db.fetchone(qry["insert"], meta.get("zipcode") or "0", json.dumps(meta))
            meta["id"] = id
            meta["hash"] = hash
            self.set(**meta)
            return self.dicted()
        self.set(id=login[0], userzip=meta.get("zipcode") or login[1], hash=login[3])
        self.set(**json.dc(login[2]))
        self.set(**meta)
        return self.dicted()

    def api_login(self, keys):
        apikey = keys.get("apikey")
        email = keys.get("email")
        if "@" in email:
            PSQL = f""" SELECT id FROM {user_table} WHERE meta @> %s """
            self.id = db.fetchreturn(PSQL, f'{{"email":"{email}"}}')
            self.session_refresh()
            return self.dicted()
        if not apikey or not email:
            self.session.delete()
            return False
        meta = {}
        PSQL = f"""SELECT id, zipcode, meta-'loyalty'-'tokens'-'storage' FROM {user_table} WHERE meta->>'email' = %s"""
        login = db.fetchone(PSQL, email)
        if login:
            meta.update(json.dc(login[2]))
        elif not login:
            self.session.delete()
            return False
        meta["lastlogin"] = nz.now()
        meta["origin"] = "apikey"
        meta["id"] = login[0]
        self.save(**meta)
        return self.dicted()

    def logout(self):
        try:
            self.session.delete()
            sleep(0.5)
            return self.session.delete()
        except Exception as e:
            logger.exception("exception logging out")
        return

    def _convert_phone_number(self, country='US'):
        if country == 'US':
            return phonenumbers.format_number(
                phonenumbers.parse(self.phone, 'US'),
                phonenumbers.PhoneNumberFormat.E164,
            )
        else:
            return phonenumbers.format_number(
                phonenumbers.parse(self.phone, country),
                phonenumbers.PhoneNumberFormat.INTERNATIONAL,
            )

    @staticmethod
    def new(meta):
        meta = json.dc(meta)
        email = meta["email"]
        u = User(email)  # see if they exist
        if u.email:
            u.save(**meta)
            return {"status": True, "id": u.id, "message": meta, "method": "user"}
        m = {
            "email": email,
            "email_verified": False,
            "display_name": meta.get("name")
            or meta.get("fullName")
            or meta["email"].split("@")[0],
            "apps" : {"dumpsterfire":"enabled"}
        }
        if "phone" in meta:
            m["phone_number"] = phonenumbers.format_number(
                phonenumbers.parse(meta.get("phone"), constants.COUNTRY_DEFAULT),
                phonenumbers.PhoneNumberFormat.E164,
            )
        if "avatar" in meta:
            m["photo_url"] = meta.get("avatar") or ""
        try:
            user = firebase_auth.create_user(**m)
            meta["fuid"] = user.uid
        except Exception as exc:
            logger.exception("Exception creating user in firebase auth")
        [meta.pop(x, "") for x in User.NEW]
        zipcode = meta.get("zipcode") or meta.get("zip") or "0"
        _id = db.fetchreturn(
            qry["upsert"], json.jc(meta), zipcode, json.jc(meta), zipcode
        )
        return {
            "status": True if _id else False,
            "id": _id,
            "message": meta if _id else "Failed to create user",
            "method": "user",
        }

    @staticmethod
    def delete(id):
        u = User(id)
        if u.fuid:
            firebase_auth.delete_user(u.fuid)
        db.execute(qry["delete"], u.id)
        logger.warning(f"User has been deleted: {u.id}")
        return u.id

    @staticmethod
    def all():
        PSQL = f""" SELECT id, hashcode, meta FROM {user_table} """
        return db.fetchall(PSQL) or []


def recaptcha(meta):
    url = constants.RECAPTCHA_KEY
    ip = request.environ.get("HTTP_X_FORWARDED_FOR") or request.environ.get(
        "REMOTE_ADDR"
    )
    if not "g-recaptcha-response" in meta:
        return False
    recaptcha = meta.pop("g-recaptcha-response")
    key = appconfig["recaptchakey"]
    postdata = {"secret": key, "response": recaptcha, "remoteip": ip}
    response = requests.get(url, params=postdata)
    result = response.json()
    if result["success"]:
        return meta
    return False


def hashcreate(password):
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt(13)).decode()

def checkpw(password, dbhash, changepw=None):
    if changepw:
        if bcrypt.hashpw(password.encode("utf8"), bcrypt.gensalt(13)) == changepw:
            return True
        else:
            return False
    if bcrypt.checkpw(password.encode("utf8"), dbhash.encode("utf8")):
        return True
    else:
        return False


def getuserhash(email):
    PSQL = """SELECT hashcode FROM users.user WHERE meta->>'email' = %s """
    return db.Pcursor().fetchreturn(PSQL, email)


def test_users():
    PSQL = '''SELECT * FROM users.user WHERE meta@> '{"utype":"test"}' '''
    return db.fetchall(PSQL)