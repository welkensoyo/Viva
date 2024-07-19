import logging
logger = logging.getLogger("AppLogger")


def add_id_hashcode(line):
    line[0]["id"] = line[1]
    line[0]["hashcode"] = line[2]
    line[0]["lastupdated"] = line[3]
    return line[0]


def API(option, client, user):
    if option == "clients":
        if user.utype == "admin" or client.name == "TriplePlayPay":
            return client.children("ALLCLIENTS")
        return client.children("CLIENTS")
    if option == "clients-meta":
        return [add_id_hashcode(x) for x in client.children("META")]