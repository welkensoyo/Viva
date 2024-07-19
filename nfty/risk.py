import logging

"""     Transaction Limits (TM)	Any ticket over $10,000
		3 transactions made on same cc within 24 hours, same merchant
		100 transactions/hr
		IP address limt=5/day
		Device =5/day
		10k Daily/50k weekly/200k Monthly Volume
		5 transactions in 24 hour period across all mids

Chargebacks	IQ
"""
import nfty.communications as email
import nfty.db as db


logger = logging.getLogger("AppLogger")

risk_table = "cache.risk"
client_table = "entity.client"

qry = {
    "get": f"""SELECT r.id, r.clientid, c.meta->>'name', r.type, r.message, r.created 
                FROM {risk_table} as r JOIN {client_table} as c ON c.id = r.clientid 
                WHERE r.id = %s or clientid = %s """,
    "clear": f"DELETE FROM {risk_table} WHERE id = %s RETURNING id",
    "new": f"INSERT INTO {risk_table} (clientid, type, message) VALUES (%s, %s, %s) RETURNING id",
    "risk": "SELECT risk_trigger() ",
}


class KYC:
    def __init__(self, meta):
        self.meta = meta

    def parse(self):
        pass

    def check_mcc(self):
        pass


class Engine:
    def __init__(self, client, meta):
        self.c = client
        self.meta = meta
        self.risks = {}

    def check(self):
        self.highticket()
        self.triggers()

    def clear(self, riskid):
        return db.fetchreturn(qry["clear"], riskid) or None

    def highticket(self):
        if self.c.config.get("high_ticket"):
            amount = float(
                self.meta.get("amount", 0) or self.meta.get("transactionAmount", 0)
            )
            if amount and amount > float(self.c.config["high_ticket"]):
                self.new(
                    "HIGHTICKET",
                    f"{self.meta['amount']} Exceeded {self.c.config['high_ticket']}",
                )
        return self

    def duplicate_cc(self):
        pass

    def cc_across_clients(self):
        pass

    def duplicate_devices(self):
        pass

    def transactions_hour(self):
        pass

    def monthly_volume(self):
        pass

    def get(self, riskid=""):
        return db.fetchall(qry["get"], riskid, self.c.id)

    def triggers(self):
        # db.execute(qry['risk'])
        pass

    def new(self, type, message):
        x = db.execute(qry["new"], self.c.id, type, message)
        if x:
            email.EmailTemplates.alert(type, message, self.c.name)
        return x


# RISK ALERT: {type} - {dba-name} - {ISV or parent}
