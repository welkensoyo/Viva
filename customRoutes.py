import logging
import nfty.db as db
import nfty.njson as json
import nfty.receipts as rcpt
import nfty.urls as shortner
from bottle import (
    Bottle,
    get,
    request,
    abort,
    route,
    template,
    redirect,
)
import nfty.constants as constants
from api.clients import qry as sqlclient, search, Client
from api._config import systemoptions
from api.users import User

logger = logging.getLogger("AppLogger")

cRoute = Bottle()
brand = systemoptions["rootname"]


@get("/app")
@get("/app/<filepath:path>")
@route("/login", method=["GET", "POST"])
@route("/sign-up", method=["GET", "POST"])
@route("/signup", method=["GET", "POST"])
@route("/sso", method=["GET", "POST"])
@route("/verify-email", method=["GET", "POST"])
def _app(filepath=""):
    logger.debug("app, login, sign-up, sso or verify-email called")
    vue = "dist/index.html"
    return template(vue)


@get("/report/<formname>")
@get("/reports/<formname>")
@get("/report/<formname>/<option>")
@get("/reports/<formname>/<option>")
@get("/report/<formname>/<option>/<option2>")
@get("/reports/<formname>/<option>/<option2>")
def _reporthandler(formname=None, option="", option2=""):
    u = User()
    if u.utype != constants.ADMIN:
        redirect("/login")
    payload = dict(request.query.decode())
    tpl = "templates/reports/%s.tpl" % (str(formname).strip().lower())
    if option:
        option, client = search(option, name_search=False)
    else:
        option, client = search(list(u.clients.keys())[0], name_search=False)
    return template(
        tpl,
        title="DUMPSTERFIRE {}".format(formname),
        user=u,
        db=db,
        formname=formname,
        apikey=u.apikey or constants.API_KEY_LOGIN,
        tk=json,
        pl=payload,
        option=option,
        option2=option2,
        msg=dict(request.query.decode()),
        url=(request.urlparts.netloc, request.urlparts.path),
        client=client,
        c=Client,
    )


@get("/admin/<formname>")
@get("/admin/<formname>/<option>")
@get("/admin/<formname>/<option>/<option2>")
def _admin(formname=None, option=None, option2=None):
    from api.clients import get_all_clients as clients

    logger.debug("Create link called")
    u = User()
    payload = dict(request.query.decode())
    logger.info(f"admin form called with payload: {payload}")
    tpl = "templates/reports/%s.tpl" % (str(formname).strip().lower())
    return template(
        tpl,
        title="TRIPLE {}".format(formname),
        user=u,
        db=db,
        formname=formname,
        apikey=u.apikey or constants.API_KEY_LOGIN,
        tk=json,
        pl=payload,
        option=option,
        option2=option2,
        msg=dict(request.query.decode()),
        url=(request.urlparts.netloc, request.urlparts.path),
        clients=clients(),
    )


@route("/logout", method=["GET", "POST"])
def logout_submit():
    logger.debug("Logout called")
    user = User()
    user.logout()
    return template("templates/logout.tpl")


@get("/receipt/<id>")
@get("/ticket/<id>")
def receipt(id=""):
    logger.debug("receipt or ticket called")
    pl = json.merge_dicts(dict(request.forms), dict(request.query.decode()))
    return rcpt.Generator(id).generate(pl or {"output": "link"})


@get("/importfile")
@get("/importfile/<method>")
def importfile(method=None):
    u = User()
    if not u.id:
        abort(403, 'Not logged in.')
    return template("templates/fileupload/importfiles.tpl", method=method or "DUMPSTERFIRE", title=method )


@get("/download/<id>")
def download(id=""):
    logger.debug(f"download called with id: {id}")
    from nfty.docs import Document

    logger.info(f"Downloading Document Id: {id}")
    return Document({}, {}).download(id)


@get("/file")
@get("/file/<clientid>")
def importfile(clientid=None):
    logger.debug("import file called")
    u = User()
    if not clientid and u.clientid:
        clientid = u.clientid
    if not clientid and not u.clientid:
        redirect("/login")
    c = Client(clientid)
    return template("templates/fileupload/clientupload.tpl", c=c, title=c.name)


@get("/url/<id>")
def shorter(id=""):
    redirect(shortner.get(id))

