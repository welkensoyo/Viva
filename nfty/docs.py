import logging
import base64
import os
import traceback
from io import BytesIO
import arrow
import boto3
from botocore.exceptions import ClientError
import nfty.db as db
from api._config import awsconfig
from nfty.njson import merge_request, jc, b64e
from nfty.tags import Tag


logger = logging.getLogger("AppLogger")

BUCKET = "tpp-documents"

qry = {
    "id": """ SELECT clientid, meta, lastupdated FROM entity.document WHERE id = %s """,
    "client": """ SELECT id, meta, lastupdated FROM entity.document WHERE clientid = %s AND NOT meta ? '_tag_' """,
    "triple": """ SELECT id, meta, lastupdated FROM entity.document WHERE clientid = %s """,
    "merchante": """ SELECT c.meta->>'name', d.meta, d.id FROM entity.document as d JOIN entity.client as c ON c.id::text=d.clientid WHERE c.meta@>'{"config":{"payment":"merchante"}}' AND d.meta ? '_tag_' UNION ALL SELECT e.meta->>'dba_name', d.meta, d.id FROM entity.document as d JOIN entity.enroll as e ON e.id::text=d.clientid WHERE e.meta@>'{"payment":"merchante"}' AND d.meta ? '_tag_' """,
    "search_triple": """ SELECT id, meta, lastupdated FROM entity.document WHERE meta @> %s AND meta ? '_tag_' """,
    "everything": """SELECT COALESCE(c.meta->>'name', e.meta->>'dba_name'), d.meta->>'filename', d.id, d.meta, TO_CHAR(d.lastupdated :: DATE, 'yyyy-mm-dd')  
FROM entity.document as d
LEFT JOIN entity.client as c on c.id::text = d.clientid
LEFT JOIN entity.enroll as e on e.id::text = d.clientid """,
    "search": """ SELECT id, meta, lastupdated FROM entity.document WHERE clientid = %s and meta @> %s AND NOT meta ? '_tag_' """,
    "new": """ INSERT INTO entity.document (clientid, meta) VALUES (%s, %s::jsonb) RETURNING id""",
    "update": """ UPDATE entity.document SET meta = meta || %s, lastupdated=now() WHERE id = %s """,
    "delete": """ DELETE FROM entity.document WHERE id = %s """,
    "tags": """ SELECT meta->>'_tag_' FROM entity.document GROUP BY meta->>'_tag_' """,
}


def get_s3_client(awsconfig):
    if os.getenv("USE_IAM_ROLE", "False").lower() in ("true", "1", "t"):
        return boto3.client("s3")
    return boto3.client(
        "s3",
        aws_access_key_id=awsconfig["accessid"],
        aws_secret_access_key=awsconfig["secret"],
    )


class Document:
    def __init__(self, request, client):
        self.request = request or {}
        self.c = client
        self.tags = set()
        self.s3 = get_s3_client()
        self.pl = merge_request(request)
        self.pl.pop("ip", "")

    def get(self, id, method=""):
        x = db.fetchone(qry["id"], id)
        if x:
            if method == "update":
                return x
            clientid, x, lastupdated = x
            file = BytesIO()
            self.s3.download_fileobj(BUCKET, f"{clientid}/{id}", file)
            file = base64.encodebytes(file.getvalue()).decode()
            if method == "link":
                return f"""<a download="{x['filename']}" href="data:text/html;base64,{file}" download>{x['filename']}</a>"""
            elif method == "download":
                return f"""<a id="dl" download="{x['filename']}" href="data:text/html;base64,{file}" download></a><script>var d = document.getElementById("dl"); d.click();</script>"""
            x["file"] = file
            return x
        return {}

    def link(self, id):
        return self.get(id, method="link")

    def download(self, id):
        return self.get(id, method="download")

    def upload(self, file):
        fullname = file.filename
        filename, self.ext = os.path.splitext(file.filename)
        self.pl.save(
            {
                "filename": fullname,
                "ext": self.ext,
                "uploaded": arrow.get().format("YYYY-MM-DD hh:mm:ss"),
            }
        )
        for key in ("apikey", "iframekey", "session"):
            self.pl.pop(key, "")
        if "mode" in self.pl:
            self.pl["_tag_"] = self.pl.pop("mode")
        if "_tag_" in self.pl:
            self.pl["_tag_"] = self.pl.get("_tag_", "").lower() or "document"
        pl = self.pl.copy()
        pl.pop("g-recaptcha-response", "")
        docid = db.fetchreturn(qry["new"], self.c.id, jc(pl))
        try:
            self.s3.upload_fileobj(
                file.file, BUCKET, f"{self.c.id}/{docid}", ExtraArgs={"Metadata": pl}
            )
        except (ClientError, AttributeError) as exc:
            logger.exception("Exception while uploading document")
            traceback.print_exc()
            db.execute(qry["delete"], docid)
            return "File Failed To Upload..."
        self.pl["id"] = docid
        return self.pl

    def new(self):
        for self.filename, file in list(self.request.files.items()):
            return self.upload(file)
        return "No file attached..."

    def update(self, meta=None):
        meta = meta or self.pl
        docid = meta.pop("id", "") or meta.pop("docid", "")
        db.execute(qry["update"], jc(meta), docid)
        return self.get(docid, method="update")

    def delete(self, docid):
        self.s3.delete_object(Bucket=BUCKET, Key=f"{self.c.id}/{docid}")
        self.s3.delete_object(Bucket=BUCKET, Key=docid)
        db.execute(qry["delete"], docid)
        return docid

    def s3delete(self, id):
        return self.s3.delete_object(Bucket=BUCKET, Key=f"{self.c.id}/{id}")

    def all(self):
        if self.pl:
            return self.search()
        return db.fetchall(qry["client"], self.c.id)

    def triple(self):
        if self.c.id == "4a731b24-0beb-457c-9585-2362c6a53cf5":
            self.pl.pop("apikey", "")
            if self.pl:
                return self.search(triple=True)
            return db.fetchall(qry["everything"])
        if self.pl:
            return self.search(triple=False)
        return db.fetchall(qry["triple"], self.c.id)

    def search(self, triple=False):
        if not triple:
            return db.fetchall(qry["search"], self.c.id, jc(self.pl))
        return db.fetchall(qry["search_triple"], jc(self.pl))

    def wipe_internal(self):
        x = db.fetchall(qry["triple"], self.c.id)
        result = []
        for e in x:
            self.delete(e[0])
            result.append(e)
        return result

    def wipe(self):
        x = db.fetchall(qry["client"], self.c.id)
        result = []
        for e in x:
            self.delete(e[0])
            result.append(e)
        return result

    def get_tags(self):
        return Tag.document(self).tags

    @staticmethod
    def save_kyc(file, filename, clientid):
        s3 = get_s3_client(awsconfig)
        meta = {
            "filename": filename,
            "ext": ".pdf",
            "uploaded": arrow.get().format("YYYY-MM-DD hh:mm:ss"),
        }
        meta["_tag_"] = "kyc"
        meta["_tag_"] = "kyc"
        if isinstance(clientid, (list, tuple)):
            clientid = clientid[0]
        docid = db.fetchreturn(qry["new"], clientid, jc(meta))
        file = base64.b64decode(file)
        file = BytesIO(file)
        s3.upload_fileobj(
            file, BUCKET, f"{clientid}/{docid}", ExtraArgs={"Metadata": meta}
        )
        meta["id"] = docid
        return meta

    @staticmethod
    def save_bin(file, filename):
        clientid = "bin"
        s3 = get_s3_client(awsconfig)
        meta = {
            "filename": filename,
            "ext": ".txt",
            "uploaded": arrow.get().format("YYYY-MM-DD hh:mm:ss"),
        }
        meta["tag"] = "bin"
        docid = db.fetchreturn(qry["new"], clientid, jc(meta))
        s3.upload_fileobj(
            file, BUCKET, f"{clientid}/{docid}", ExtraArgs={"Metadata": meta}
        )
        meta["id"] = docid
        return meta

    @staticmethod
    def dict2file(text):
        file = BytesIO(jc(text, indent=4).encode())
        file.seek(0)
        return b64e(file.read().decode())

    def purge(self, clientid):
        PSQL = """SELECT id FROM entity.document WHERE clientid = %s"""
        x = db.fetchall(PSQL, clientid)
        for file in x:
            self.delete(file[0])
        return x

    def merchante(self):
        return db.fetchall(qry["merchante"])
