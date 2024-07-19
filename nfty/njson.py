import base64
import json as _json
import traceback
import xmltodict
from uuid import UUID, uuid4
import logging
import arrow
import orjson
import ujson as json
import nfty.constants as constants


logger = logging.getLogger("AppLogger")


def ctnow():
    return arrow.get().to(constants.DEFAULT_TIMEZONE)


def b64e(meta):
    return base64.urlsafe_b64encode(str(meta).encode())


def b64d(meta):
    return base64.urlsafe_b64decode(str(meta).encode())


class JsonError(Exception):
    pass


def jc(data, indent=None):  # convert to json
    if isinstance(data, (tuple, list, set, dict)):
        try:
            if indent:
                return json.dumps(data, indent=indent)
            return json.dumps(data)
        except TypeError:
            return _json.dumps(data, default=str)
        except Exception as exc:
            logger.exception("Exception with jc")
            traceback.print_exc()
            return "{}"
    elif isinstance(data, (bytes)):
        return data.decode()
    elif data:  # assume properly formatted json string
        return data
    else:  # or if there isn't any data for some reason
        return "{}"


def jsonprint(data):
    return json.dumps(data, indent=4)


def jsonhtml(data):
    data.pop("client_tc", "")
    return f"<pre>{json.dumps(data, indent=4)}</pre>"


def lc(data):  # convert to list
    if isinstance(data, list):
        return data
    elif isinstance(data, (str, bytes)):
        try:
            return json.loads(data)
        except Exception as exc:
            logger.exception("Exception with lc")
            traceback.print_exc()
            return []


def dc(data, only=False):  # convert to dict
    if isinstance(data, (dict)):
        return data
    if isinstance(data, (bytes)):
        data = data.decode()
    if isinstance(data, (str)):
        if "{" in data:
            try:
                try:
                    return json.loads(data)
                except:
                    return _json.loads(data)
            except Exception as e:
                logger.exception("Exception thrown with dc")
                traceback.print_exc()
                pass
    if only:
        return {}
    return data


# convert csv to json
def csj(result):
    if not result:
        return None
    d = []
    if len(result) > 1:
        for each in result:
            jstring = each[0].replace("'", '"')
            j = json.loads(jstring)
            j.save({"id": "%s" % each[1]})
            d.append(j)
        return json.dumps(d)
    else:
        each = result[0][0]
        jstring = each.replace("'", '"')
        j = json.loads(jstring)
        j.save({"id": "%s" % result[0][1]})
        return json.dumps(j)


def merge_dicts(*args):
    result = {}
    for dictionary in args:
        result.update(dictionary)
    return result


def merge_request(request):
    if isinstance(request, (dict)):
        return request
    return dc(request.json) or merge_dicts(
        dict(request.forms), dict(request.query.decode())
    )


def checkuuid(u, version=4):
    try:
        UUID(u, version=version)
        return u
    except:
        return ""


def newid():
    return str(uuid4())


def odumps(data):
    data = orjson.dumps(data, default=str)
    return data.decode()


def clean_date(t):
    if "-" in t:
        return t
    else:
        return arrow.get(t, "M/D/YYYY").format("YYYY-MM-DD")


def xml(meta):
    return json.loads(json.dumps(xmltodict.parse(meta)))


def toxml(meta):
    return xmltodict.unparse(dc(meta))


def toxml_short(meta):
    return xmltodict.unparse(dc(meta), short_empty_elements=True)


oloads = orjson.loads
dumps = json.dumps
loads = json.loads
