import base64
import zlib
import logging
import arrow
from cryptography.fernet import Fernet
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
import nfty.njson as json
from api._config import appconfig


logger = logging.getLogger("AppLogger")

SALT = appconfig["SALT"]
SALT_SIZE = appconfig["SALT_SIZE"]
SALT_OFF_SET = appconfig["SALT_OFF_SET"]
NUMBER_OF_ITERATIONS = appconfig["NUMBER_OF_ITERATIONS"]
AES_MULTIPLE = appconfig["AES_MULTIPLE"]


# cryptography
def cryptkey(password=""):
    digest = hashes.Hash(hashes.SHA256(), backend=default_backend())
    digest.update(bytes(SALT + password, "utf-8"))
    return Fernet(base64.urlsafe_b64encode(digest.finalize()))


def encrypt(meta, password=""):
    if not meta:
        return meta
    meta = json.jc(meta).encode()
    meta = zlib.compress(meta, 9)
    f = cryptkey(password)
    return base64.urlsafe_b64encode(f.encrypt(bytes(meta))).decode()


def decrypt(meta, password=""):
    if not meta:
        return meta
    meta = base64.urlsafe_b64decode(meta)
    f = cryptkey(password or "")
    meta = f.decrypt(bytes(meta))
    meta = zlib.decompress(meta)
    return json.loads(meta)


def pad_text(text, multiple):
    extra_bytes = len(text) % multiple
    padding_size = multiple - extra_bytes
    padding = chr(padding_size) * padding_size
    padded_text = text + padding
    return padded_text


def unpad_text(padded_text):
    padding_size = ord(padded_text[-1])
    text = padded_text[:-padding_size]
    return text


def create_session(apikey, expires):
    f = Fernet(appconfig["iframe"])
    meta = {
        "apikey": apikey,
        "expires": arrow.get().shift(minutes=int(expires)).format(),
    }
    return f.encrypt(json.jc(meta).encode()).decode()


def check_session(session):
    try:
        f = Fernet(appconfig["iframe"])
        x = json.dc(f.decrypt(session.encode()).decode())
        if arrow.get() < arrow.get(x["expires"]):
            return x["apikey"]
    except Exception as e:
        logger.exception("Exception raised while checking session")
        return False
    return False
