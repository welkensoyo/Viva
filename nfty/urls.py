import os
import logging
import nfty.db as db
from nfty.njson import checkuuid
from api._config import env


logger = logging.getLogger("AppLogger")


qry = {
    "get": """ SELECT url FROM cache.urls WHERE id = %s """,
    "new": """ INSERT INTO cache.urls (url) VALUES (%s) RETURNING id """,
}


def new(url):
    if not url:
        return None
    id = db.fetchreturn(qry["new"], url)
    if env == "prod":
        return f"https://tripleplaypay.com/url/{id}"

    # TODO - Update code to strictly use environment variable to set domain
    if os.getenv("NON_PROD_ENV") and os.getenv("NON_PROD_ENV").lower() in ["true", "1"]:
        domain = os.getenv("DOMAIN")
        return f"https://{domain}/url/{id}"

    return f"https://sandbox.tripleplaypay.com/url/{id}"

def get(id):
    return db.fetchreturn(qry["get"], id)


def detect(x):
    if checkuuid(x):
        return get(x)
    return new(x)
