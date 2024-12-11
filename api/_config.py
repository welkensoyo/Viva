import os
import sys
import logging
from types import SimpleNamespace

dsn = "host='nfty.asuscomm.com' port='5555' dbname='viva' user='nfty' password='Viva123!!' connect_timeout=2 application_name=VIVA"

logger = logging.getLogger("ConfigLogger")

HOST_NAME = ""
env = os.getenv("ENV", 'localhost')
if not dsn:
    dsn = os.getenv("DSN")
# dsn = b64d(dsn).decode()
psqldsn = dsn.replace('"', "'")

_c = {"empty": True, "_": {}}
awsconfig = {}
smtp = {}

# def load():  # load initial config from database
#     global _c
#     global documentation
#     import psycopg2
#
#     conn = psycopg2.connect(dsn=psqldsn)
#     cur = conn.cursor()
#     cur.execute("SELECT settings FROM cache.config WHERE app = 'dumpsterfire' ")
#     _c = cur.fetchone()[0]
#     cur.close()
#     conn.close()
#
#
# if _c.get("empty"):
#     load()

session_cookie_domain = "localhost"
g_search_id = 'c57432cfa88f94726'
g_search_key = 'AIzaSyAD0mkfSCLfuXOYqqyjreQsGXV_2DXSGL8'
systemoptions = _c.get("systemoptions",{})
beakerconfig = {
    "session.type": "cookie",
    "session.key": "session_id",
    "session.cookie_domain": session_cookie_domain,
    "session.invalidate_corrupt": True,
    "session.cookie_path": "/",
    "session.auto": True,
    "session.timeout": 2592000,
    "session.httponly": True,
    "session.crypto_type": "cryptography",
    "session.secret": "8b1040cb-06e3-4372-a2dd-49aab5012e3a",
    "session.encrypt_key": "8b1040cb-06e3-4372-a2dd-49aab5012e3a",
    "session.validate_key": "8b1040cb-06e3-4372-a2dd-49aab5012e3a",
    "session.cookie_expires": False,
}

appconfig = {
    'SALT' : '$2b$12$FTp8e/eGklyVLMgpp1qiXe',
    'SALT_SIZE' : 42,
    'SALT_OFF_SET' : 1,
    'NUMBER_OF_ITERATIONS' : 20,
    'AES_MULTIPLE' : 16,
}

try:
    from gevent import socket
    HOST_NAME = socket.gethostname()
    # print(HOST_NAME)
except:
    pass

paycor = SimpleNamespace(
    clientid = 'cbe097ae418b0d8b1850',
    secret = 'lBpKk0yyzZmprMCmqARNxNqBlh0x6ugswhwdQ3AFFQ'
)
# os.environ["ANTHROPIC_API_KEY"] = anthropic_key
_c = {}

PORT = 80
if HOST_NAME.lower() in ("nfty", "djbartron-lap", "dereks-mbp"):
    print('DEV MODE')
    HOST_NAME = "localhost"
    PORT = 443
    beakerconfig.update({"session.cookie_domain": "192.168.50.28"})
    systemoptions["appserver"] = "http://{0}:{1}".format(HOST_NAME, PORT)
    systemoptions["apiserver"] = "/nfty"
    systemoptions["rootserver"] = "http://{0}:{1}".format(HOST_NAME, PORT)
    systemoptions["rootdomain"] = "http://localhost"
    systemoptions["websockurl"] = "ws://{}/ws/app".format(HOST_NAME)
    systemoptions["websockurlpos"] = "ws://{}/ws/pos".format(HOST_NAME)
    systemoptions["staticfolder"] = "static"
    systemoptions["ip"] = "54.87.136.153"  # 54.186.136.153
    systemoptions["ssl"] = True
    systemoptions["loglevel"] = "DEBUG"
    systemoptions["compress"] = True

# print(sys.version)
# print(env)
