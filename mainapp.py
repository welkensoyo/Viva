import traceback
import arrow
import logging
import api.api as a
import nfty.njson as json
import nfty.events as events
from gevent import Timeout, sleep
from geventwebsocket import WebSocketError
from bottle import (
    Bottle,
    get,
    request,
    response,
    abort,
    route,
    error,
    template,
    redirect,
    static_file,
    hook,
)
import nfty.constants as constants
from api.clients import Client
from api.users import User

logger = logging.getLogger("AppLogger")
mainappRoute = Bottle()
ws_connections = a.ws_connections
@hook("after_request")
def enable_cors():
    response.set_header("Allow", "PUT, GET, POST, DELETE, OPTIONS, payment")
    response.add_header("Access-Control-Allow-Origin", "*")
    response.add_header(
        "Access-Control-Allow-Methods", "PUT, GET, POST, DELETE, OPTIONS"
    )
    response.add_header(
        "Access-Control-Allow-Headers",
        "Origin, Accept, Content-Type, X-Requested-With, X-CSRF-Token, Authorization, X-Auth-Token",
    )


@route("/favicon.ico")
def serve_icon():
    return static_file("favicon.ico", root="static/images/")


@get("/releasenotes")
def rnotes():
    logger.info("Someone is checking our release notes!")
    redirect("https://github.com/TriplePlayPay/API/releases")


@get("/healthcheck")
@get("/healthcheck/index.html")
@get("/index.html")
def hcheck():
    logger.info("Healthcheck Pinged")
    return "Online..."


@get("/")
def _index():
    if "ELB-HealthChecker" in ua:
        return "Online..."
    user = User()
    redirect("/app/login")


@route("/api/<command>", method=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"])
@route("/api/<command>/<option>", method=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"])
@route("/api/<command>/<option>/<option2>", method=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"])
def _api(command=None, option="", option2=""):
    # Establish response headers
    response.headers["Content-Type"] = "application/json"
    response.headers["Cache-Control"] = "no-cache"

    # Return an empty dictionary string
    if request.method == "OPTIONS":  # prefetch CORS POST
        return "{}"
    query = json.merge_dicts(dict(request.forms), dict(request.query.decode()))
    apikey = (
        request.headers.get("Authorization", "")
        .replace("bearer", "")
        .replace("Bearer", "")
        .replace("BEARER", "")
        .strip()
        or query.pop("apikey", None)
        or query.pop("sessionkey", None)
    )
    payload = json.dc(request.json) or query
    # if apikey and not json.checkuuid(apikey):
    #     apikey = Client.session(apikey)
    if not apikey:
        abort(401, constants.CHECK_API_SPECIFICATIONS)
    payload["_method_"] = option
    payload["_method2_"] = option2

    # Parse the API Gateway for the relevant request
    wapi = a.API(payload, apikey, request.environ, request.method.lower())
    func = getattr(wapi, "{}".format(command), None)
    try:
        if callable(func):
            result = func()
            if result:
                json_result = json.jc(result)
                logger.debug(f"returning {json_result}")
                return json_result
            else:
                logger.debug(f"returning {json.jc({})}")
                return json.jc({})
    except KeyError as exc:
        logger.exception(f"""  ********* ERROR {arrow.get().format('')} ********""")
        logger.exception(apikey)
        logger.exception(payload)
        logger.exception(traceback.print_exc())
        sleep(1)
        logger.error(f"Missing {exc}")
        return json.jc({"status": False, "message": "Missing " + str(exc)})
    except Exception as exc:
        logger.exception(f"""  ********* ERROR {arrow.get().format('')} ********""")
        logger.exception(apikey)
        logger.exception(payload)
        logger.exception(traceback.print_exc())
        sleep(5)
        return json.jc({"status": False, "message": constants.CHECK_API_SPECIFICATIONS})


# websocket
@route("/ws/api")
@route("/ws")
def handle_websocket():
    global ws_connections
    u = User()
    logger.debug("Websocket called")
    ws = request.environ.get("wsgi.websocket")
    if not ws:
        abort(400, "Expected WebSocket request.")
    # query = json.merge_dicts(dict(request.forms), dict(request.query.decode()))
    ws_connections[u.id] = {'user': u, 'ws': ws}
    while 1:
        message = None
        try:
            with Timeout(2, False) as timeout:
                message = ws.receive()
            if message:
                # print(message)
                message = json.dc(message)
                if '_ping_' in message:
                    ws.send({'_pong_': True})
                    continue
                mode = message.pop("_mode_", "get")
                for command, payload in message.items():
                    wapi = a.API(json.dc(payload, only=True), u, request.environ, mode=mode)
                    func = getattr(wapi, command, None)
                    if callable(func):
                        try:
                            x = func()
                            ws.send(json.jc({command: x}))
                        except KeyError as exc:
                            logger.exception("KeyError calling function")
                            traceback.print_exc()
                            ws.send(
                                json.jc(
                                    {
                                        command: {
                                            "status": False,
                                            "message": f"Missing {exc}",
                                        }
                                    }
                                )
                            )
                        except Exception as exc:
                            logger.exception("Websocket Exception calling function")
                            traceback.print_exc()
                            sleep(5)
                            ws.send(
                                json.jc(
                                    {command: {"status": False, "message": str(exc)}}
                                )
                            )
        except WebSocketError:
            break
        except Exception as exc:
            traceback.print_exc()
            sleep(3)
    ws_connections.pop(u.id, '')
    abort(400, "Closed Connection.")

@route("/callback/<option>", method=["GET", "POST", "PUT", "PATCH", "DELETE"])
def callback(option=None):
    if option:
        meta = json.merge_dicts(dict(request.forms), dict(request.query.decode()))
        return events.callbackurl(option, meta)


@error(404)
@error(400)
def error400s(error):
    tpl = "templates/error.tpl"
    return template(tpl, msg="ERROR : 404", body=error.body)


@error(500)
def error500s(error):
    tpl = "templates/error.tpl"
    logger.error(error)
    logger.error("A 500 was returned from the server, details above")
    return template(tpl, msg="ERROR: 500", body=error.body)
