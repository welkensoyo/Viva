import logging
import nfty.db as db
from nfty.communications import alert as notify
from nfty.njson import jc


logger = logging.getLogger("AppLogger")

qry = {
    "get": """ SELECT id, clientid, type, message, created FROM cache.operations WHERE created >= date_trunc('month', current_date - interval '1' month) """,
    "new": """ INSERT INTO cache.operations (clientid, type, meta) VALUES (%s,%s,%s); """,
}


def new(clientid, tag, message):
    try:
        message = jc(message)
        db.execute(qry["new"], clientid, tag, message)
        notify(f"{tag} : {clientid}", message)
    except Exception as e:
        logger.exception("Exception with new operation")
    return
