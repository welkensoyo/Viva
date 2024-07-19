import logging
import traceback
from typing import Dict
from time import perf_counter
from gevent import spawn as gspawn
import nfty.db as db
import nfty.constants as constants
from nfty.communications import do
from api._config import systemoptions
from nfty.njson import checkuuid, jc
from nfty.notes import Notes

logger = logging.getLogger("AppLogger")
STATIC_FEES = (0.5)


def logtran(clientid: str, meta: Dict) -> None:
    logger.info(f"Logging transaction: {jc(meta)}")
    db.execute(
        f"""INSERT INTO {systemoptions['transaction']} (clientid,meta) VALUES (%s,%s)""",
        clientid,
        jc(meta),
    )


def logtrandate(clientid, meta, created):
    logger.info(f"Logging transaction date: {jc(meta)}")
    db.execute(
        f"""INSERT INTO {systemoptions['transaction']} (clientid,meta,created) VALUES (%s,%s,%s)""",
        clientid,
        jc(meta),
        created,
    )


def logtest(clientid: str, meta: Dict) -> None:
    logger.info(f"Logging test metadata {jc(meta)}")
    try:
        db.execute(
            f"""INSERT INTO {systemoptions['testtransaction']} (clientid,meta) VALUES (%s,%s)""",
            clientid,
            jc(meta),
        )
    except Exception as e:
        logger.exception("Exception encountered while logging test transaction")
        traceback.print_exc()


def logclient(clientid, meta):
    logger.info(f"Logging client metadata: {jc(meta)}")
    db.execute(
        f"""INSERT INTO {systemoptions['config_history']} (clientid, meta) VALUES (%s,%s)""",
        clientid,
        jc(meta),
    )


def _log(f):
    def d(self):
        s = perf_counter()
        r = f(self)
        e = perf_counter()
        if isinstance(self.pl, str):
            self.pl = {"status": False, "message": self.pl}
            logger.debug(f"Payload is an instance of a string, returning: {self.pl}")
            return self._response(*r)
        self.pl["ip"] = self.pl.get("ip", self.request_details.get("REMOTE_ADDR"))
        self.pl["status"] = self.pl.get("status", True)
        try:
            method = r[2]
            id = r[0]
        except Exception as e:
            logger.exception(
                f"Exception encountered with method: {f.__name__} and client id: {self.c.id}"
            )
            method = f.__name__
            id = self.c.id
        if len(self.pl.keys()) < 5 or (
            method in ("charge", "authorize")
            and (not self.pl.get("amount") or self.pl.get("amount") == "0")
        ):
            return self._response(*r)
        header = {"latency": e - s, "request": self.request_details}
        response = {
            "method": method,
            "id": id,
            "clientId": self.c.id,
            "dbaName": self.c.name,
        }
        logger.info(f"Response: {response}")
        if method == "crypto":
            self.pl[
                "status"
            ] = False  # this is to set the failure if the user never finishes the transaction
            if self.c.callback:
                gspawn(do, self.c.callback, self.pl)
            gspawn(logtran, self.c.id, {**self.pl, **header, **response})
            self.pl["status"] = True
            return self._response(*r)
        elif method == "bulk":
            return self._response(*r)
        elif self.c.apikey == constants.API_KEY_TEST:
            gspawn(logtest, self.c.id, {**self.pl, **header, **response})
        elif checkuuid(self.c.id):
            gspawn(logtran, self.c.id, {**self.pl, **header, **response})
        return self._response(*r)

    return d


def _log_simple(self):
    gspawn(
        Notes.new,
        self.c.id,
        self.u.id or self.c.id,
        constants.SYSTEM_USER,
        jc({**self.pl}),
    )


def _clientlog(f):
    def d(self):
        s = perf_counter()
        r = f(self)
        e = perf_counter()
        self.pl["method"] = r[2] or f.__name__
        header = {"latency": e - s, "request": self.request_details}
        if checkuuid(self.c.id):
            gspawn(logclient, self.c.id, {**self.pl, **header})
        return self._response(*r)

    return d


qry = {}


class Logs:
    def __init__(self, timezone=constants.DEFAULT_TIMEZONE):
        self.timezone = timezone
        self.variance = 2.00

    def userlinks(self):
        return db.fetchall(qry["userlinks"])

    def _set_tz(self):
        return f""" SET TIME ZONE '{self.timezone}'; """

    def ticketid(self, id, ticketid):
        id = id if isinstance(id, str) else id[0]
        return self._fetch(id, f'{{"id":"{ticketid}" }}')

    def reference(self, id, reference):
        id = id if isinstance(id, str) else id[0]
        return self._fetch(
            id, f'{{"details":{{"Transaction":{{"ReferenceNumber":"{reference}" }}}}}}'
        )

    def transid(self, id, transactionid):
        id = id if isinstance(id, str) else id[0]
        return self._fetch(
            id,
            f'{{"details":{{"Transaction":{{"TransactionID":"{transactionid}" }}}}}}',
        )

    def retrieval(self, id, retrievalid):
        id = id if isinstance(id, str) else id[0]
        return self._fetch(
            id,
            f'{{"details":{{"Transaction":{{"RetrievalReferenceNumber":"{retrievalid}" }}}}}}',
        )

    def ticket(self, id):
        return db.fetchone(qry["ticket"].format(id, self.timezone))

    def _fetch(self, id, tid):
        if id == "TRIPLE":
            PSQL = """SELECT meta FROM {0} WHERE meta @> %s::jsonb """.format(
                systemoptions["transaction"]
            )
            return db.fetchone(PSQL, tid)
        PSQL = """SELECT meta FROM {0} WHERE id = %s AND meta @> %s::jsonb """.format(
            systemoptions["transaction"]
        )
        return db.fetchone(PSQL, id, tid)
