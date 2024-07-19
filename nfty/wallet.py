import re
import traceback
import logging
from typing import Dict
import cryptography.fernet
import nfty.db as db
import nfty.ncrypt as crypt
import nfty.njson as json
from api._config import systemoptions
from nfty.communications import bank_info


logger = logging.getLogger("AppLogger")

customer_table = systemoptions["customer"]
client_table = systemoptions["client"]
token_table = systemoptions.get("tokens") or "entity.token"

qry = {
    "hash": """SELECT id, hashcode, meta->'wallet', meta->'tokens', meta->>'id' FROM {0} WHERE hashcode = %s """,
    "search": """SELECT id, hashcode, meta->'wallet', meta->'tokens', meta->>'id' FROM {0} WHERE id::text = %s or hashcode = %s or meta @> %s::jsonb """,
    "id": """SELECT id, hashcode, meta->'wallet', meta->'tokens', meta->>'id' FROM {0} WHERE id::text = %s """,
    "new": """ INSERT INTO {} (meta, zipcode) VALUES (%s, %s) RETURNING id, hashcode """,
    "universal": """ INSERT INTO {} (id, hashcode, meta) VALUES (%s, %s, %s) ON CONFLICT (id) DO UPDATE SET meta = token.meta || %s, created=now() RETURNING id """,
    "new_token": """ INSERT INTO {} (id, meta) VALUES (%s, %s) returning hashcode """,
    "get_token": """ SELECT entity.tokenhash() """,
}


class Wallet:
    def __init__(self, _id, client=False):
        self._table = customer_table if not client else client_table
        self.id, self.hashcode, self.wallet, self.tokens = _id or "", None, [], {}
        self.token = None
        self.tokenid = ""
        self.get()
        if not self.wallet:
            self.tokens = self.tokens or {}
            self.wallet = list(self.tokens.keys())

    def get(self):
        x = None
        u = None
        if json.checkuuid(self.id):
            x = db.fetchone(qry["id"].format(self._table), self.id)
            if not x:
                userid = f"""{{"id":"{self.id}"}}"""
                u = db.fetchone(
                    qry["search"].format(token_table), self.id, self.id, userid
                )
        elif (
            self.id and len(self.id) > 8 and self.id[8] == "-" and "*" in self.id
        ):  # standalone token
            u = db.fetchone(qry["hash"].format(token_table), self.id[0:8])
            self.token = self.id
        elif (
            self.id and len(self.id) > 6 and self.id[6] == "-" and "*" in self.id
        ):  # incase they pass in token for lookup
            x = db.fetchone(qry["hash"].format(self._table), self.id[0:6])
            self.token = self.id
        elif self.id and (not x and not u):  # do flexible search
            if "@" in self.id:
                email = f'{{"email":"{self.id}"}}'
                x = db.fetchone(
                    qry["search"].format(self._table), self.id, self.id, email
                )
            else:
                userid = f"""{{"id":"{self.id}"}}"""
                u = db.fetchone(
                    qry["search"].format(token_table), self.id, self.id, userid
                )
                if not u and not x:
                    phone = "".join([s for s in str(self.id) if s.isdigit()])
                    if phone:
                        phone = f"""{{"phone":"{phone}"}}"""
                        x = db.fetchone(
                            qry["search"].format(self._table), self.id, self.id, phone
                        )
        if x:
            self.id, self.hashcode, self.wallet, self.tokens, self.tokenid = x
        elif u:
            self._table = token_table
            self.id, self.hashcode, self.wallet, self.tokens, self.tokenid = u
            if "*" in self.id and "META" in self.id:
                self.token = self.id
                self.id = self.id.split("*")[1]
        else:
            self.new()

    def new(self, meta=None):
        meta = meta or {}
        if "@" in self.id:
            meta["email"] = self.id
            self.id, self.hashcode = db.fetchone(
                qry["new"].format(self._table), json.jc(meta), meta.get("zipcode", "0")
            )
        else:
            self._table = token_table

    def _exists(self, keys, meta):
        return all(k in meta for k in (keys))

    def luhn(self, cc):
        cc = [int(ch) for ch in str(cc)][::-1]
        return (sum(cc[0::2]) + sum(sum(divmod(d * 2, 10)) for d in cc[1::2])) % 10 == 0

    def alias(self, meta: Dict) -> str:
        """
        Provides an alias with the given metadata.

        Args:
            meta: Dict

        Returns:
            Dict
        """
        if "routingNumber" in meta:
            return "{}-BANK:*{}".format(self.hashcode, meta["accountNumber"][-4:])
        cc = meta["cc"].replace("-", "").replace(" ", "")
        amex_re = re.compile(r"^3[47][0-9]{13}$")
        visa_re = re.compile(r"^4[0-9]{12}(?:[0-9]{3})?$")
        mc_re = re.compile(r"^5[1-5][0-9]{14}$")
        discover_re = re.compile(r"^6(?:011|5[0-9]{2})[0-9]{12}$")
        maestro_re = re.compile(r"^(?:5[0678]\d\d|6304|6390|67\d\d)\d{8,15}$")
        ccmap = {
            "AMEX": amex_re,
            "VISA": visa_re,
            "MC": mc_re,
            "DISCOVER": discover_re,
            "MAESTRO": maestro_re,
        }
        for typ, regexp in list(ccmap.items()):
            if regexp.match(str(cc)) and self.luhn(cc):
                if typ != "AMEX":
                    ccalias = "{}-{}:*{}".format(self.hashcode, typ, cc[-4:])
                else:
                    ccalias = "{}-AMEX:*{}".format(self.hashcode, cc[11:15])
                return ccalias
        return "{}-CC:*{}".format(self.hashcode, cc[-4:])

    def universal(self, metax: Dict):
        meta = metax.copy()
        meta.pop("amount", "")
        try:
            if not self.hashcode:
                self.hashcode = db.fetchreturn(qry["get_token"])
            if self._exists(("account_number", "routing_number"), meta):
                meta["accountNumber"] = meta.pop("account_number")
                meta["routingNumber"] = meta.pop("routing_number")
            if self._exists(("routingNumber", "accountNumber"), meta) or self._exists(
                ("cc", "yy", "mm"), meta
            ):
                token = self.alias(meta)
            else:
                self.id = self.id or json.newid()
                token = "{}-{}:{}".format(self.hashcode, "META", self.id)
            self.tokens.update({token: crypt.encrypt(meta, token)})
            self.wallet = list(self.tokens.keys())
            jsn = json.jc({"wallet": self.wallet, "tokens": self.tokens, "id": self.id})
            x = db.fetchreturn(
                qry["universal"].format(token_table), token, self.hashcode, jsn, jsn
            )
            if x:
                return token
        except Exception as e:
            logger.exception("Exception while serializing token in wallet")
            traceback.print_exc()
            return ""

    def clean(self, meta):
        m = meta.copy()
        for i in list(meta.keys()):
            if i not in (
                "cc",
                "mm",
                "yy",
                "yyyy",
                "cvv",
                "ccv",
                "cvv2",
                "accountNumber",
                "account_number",
                "routingNumber",
                "routing_number",
                "account_type",
                "accountType",
            ):
                m.pop(i)
        return m

    def tokenize(self, metax: Dict) -> str:
        """
        Tokenizes the given dictionary value into a string.

        Args:
            metax: Dict

        Returns:
            str
        """
        try:
            if self._table == token_table:
                return self.universal(metax)
            else:
                meta = metax.copy()
                meta.pop("amount", "")
            if self._exists(("account_number", "routing_number"), meta):
                meta["accountNumber"] = meta.pop("account_number")
                meta["routingNumber"] = meta.pop("routing_number")
            if (
                self._exists(("routingNumber", "accountNumber"), meta)
                or self._exists(("cc", "yy", "mm"), meta)
                and self.hashcode
            ):
                token = self.alias(meta)
                self.tokens.update({token: crypt.encrypt(meta, self.id)})
                self.wallet = list(self.tokens.keys())
                self.save()
                return token
            else:
                if len(meta.keys()) > 2:
                    token = "{}-{}:*{}".format(self.hashcode, "META", json.newid())
                    self.tokens.update({token: crypt.encrypt(meta, self.id)})
                    self.wallet = list(self.tokens.keys())
                    self.save()
                    return token
        except Exception as e:
            logger.exception("Exception in wallet tokenize method")
            traceback.print_exc()
            return ""

    def default(self, token):
        self.wallet.insert(0, self.wallet.pop(token))
        return self.wallet

    def detokenize(self, token: str = "") -> Dict:
        """
        Detokenize a given asset and return it as a dictionary.

        Args:
            token: str

        Returns:
            Dict
        """
        result = {}
        token = token or self.token or self.id
        if self.id and self.wallet and token in self.tokens:
            try:
                result.update(crypt.decrypt(self.tokens[token], self.id))
            except cryptography.fernet.InvalidToken:
                try:
                    result.update(crypt.decrypt(self.tokens[token], self.token))
                except cryptography.fernet.InvalidToken:
                    result.update(crypt.decrypt(self.tokens[token], self.tokenid))
        elif self.id and self.wallet:  # grab default wallet
            try:
                result.update(crypt.decrypt(self.tokens[self.wallet[0]], self.id))
            except cryptography.fernet.InvalidToken:
                result.update(crypt.decrypt(self.tokens[self.wallet[0]], self.token))
        result.pop("id", "")
        result.pop("frequency", "")
        if "cvv" in result:
            try:
                int(result["cvv"])
            except Exception as e:
                logger.exception("Issues tokenizing CVV in wallet")
                result.pop("cvv")
        return result

    def retrieve(self, token):
        x = self.detokenize(token=token)
        for _ in ("cc", "yy", "mm", "cvv", "accountNumber", "routingNumber"):
            x.pop(_, "")
        return x

    def remove(self, token):
        self.tokens.pop(token)
        self.wallet = list(self.tokens.keys())
        return self.save()

    def save(self):
        db.updatemeta(
            self._table, self.id, {"wallet": self.wallet, "tokens": self.tokens}
        )
        return self

    def achupdate(self, meta, reset=True):
        if meta and meta.get("routingNumber") and meta.get("accountNumber"):
            if reset:
                self.wallet = []
                self.tokens = {}
            x = {
                "accountNumber": meta.pop("accountNumber"),
                "routingNumber": meta.pop("routingNumber"),
                "accountType": meta.pop("accountType", "checking").lower(),
            }
            x.update(bank_info(x["routingNumber"]))
            x.pop("rn", "")
            self.tokenize(x)
        return meta

    def __repr__(self):
        return self.wallet

    def __str__(self):
        return json.dumps(self.wallet)

    @staticmethod
    def migrate():
        PSQL = "SELECT id, meta FROM entity.enroll"
        x = db.fetchall(PSQL)
        for id, meta in x:
            a, r, t = (
                meta.get("account_number"),
                meta.get("routing_number"),
                meta.get("account_type"),
            )
            if a and r:
                Wallet(id, client=True).tokenize(
                    {"accountNumber": a, "routingNumber": r, "accountType": t.lower()}
                )


if __name__ == "__main__":
    w = Wallet("Vk5QjY-VISA:*6825")
    print(w.detokenize())
