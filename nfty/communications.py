# -*- coding: utf-8 -*-
import smtplib
import ssl
import traceback
from phonenumbers import carrier, parse as pparse
import certifi
import urllib3
import logging
from collections import UserDict
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from ssl import CERT_NONE
from gevent import spawn
from bottle import template
from api._config import smtp, systemoptions, env
from nfty.njson import jc, dc
import nfty.constants as constants

logger = logging.getLogger("AppLogger")

retries = urllib3.util.Retry(connect=5, read=3, redirect=2, backoff_factor=0.05)
upool = urllib3.PoolManager(
    cert_reqs="CERT_REQUIRED",
    ca_certs=certifi.where(),
    num_pools=20,
    block=False,
    retries=retries,
)
apple_pool = urllib3.PoolManager(
    cert_reqs="CERT_REQUIRED",
    ca_certs=certifi.where(),
    cert_file="keys/apple_mid.crt.pem",
    key_file="keys/apple_mid.key.pem",
    num_pools=7,
    block=False,
)

client_table = systemoptions["client"]
customer_table = systemoptions["customer"]
brand = systemoptions["rootname"]
rootserver = systemoptions.get("rootserver","")
bcc = f""
cc = ""

qry = {
    "client": f""" SELECT meta->>'dba_name', meta->>'email', meta->>'website', meta->>'callback' FROM {client_table} WHERE id::text = %s or meta @> %s::jsonb """,
    "customer": f""" SELECT meta->>'name', meta->>'email' FROM {customer_table} WHERE id::text = %s or meta @> %s::jsonb """,
}

SMS_Providers = {
    "AT&T": ("@txt.att.net", "@mms.att.net"),
    "Boost Mobile": ("@sms.myboostmobile.com", "@myboostmobile.com"),
    "Cricket Wireless": ("@sms.cricketwireless.net", "@mms.cricketwireless.net"),
    "T-Mobile": ("@tmomail.net", "@tmomail.net"),
    "UScellular": ("@email.uscc.net", "@mms.uscc.net"),
    "Verizon": ("@vtext.com", "@vzwpix.com"),
}


class ConnectionTimeout(Exception):
    pass


def bank_info(routing_number):
    if len(routing_number) == 9:
        url = f"https://www.routingnumbers.info/api/data.json?rn={routing_number}"
        r = upool.request("GET", url)
        return dc(r.data.decode())
    return {}


def send(
        subject,
        body,
        dest,
        sender=smtp.SMTPUSERNAME,
        password=smtp.SMTPPASSWORD,
        attachment=None,
        priority=False,
        cc="",
        bcc="",
        server=smtp.SMTPSERVER,
        port=587,
        template=None,
):
    if env == constants.PRODUCTION:
        spawn(
            email,
            subject,
            body,
            dest,
            sender,
            password,
            attachment,
            priority,
            cc,
            bcc,
            server,
            port,
        )
    else:
        email(
            subject,
            body,
            constants.EMAIL_TEAM,
            sender,
            password,
            attachment,
            priority,
            "",
            "",
            server,
            port,
        )
    return


def alert(
        subject,
        body,
        dst=constants.EMAIL_DEV_TEAM,
        sender=smtp.SMTPUSERNAME,
        password=smtp.SMTPPASSWORD,
        attachment=None,
        priority=False,
        cc="",
        bcc="",
        server=smtp.SMTPSERVER,
        port=587,
):
    """
    Spawns a new instance of `email` to send.
    """
    spawn(
        email,
        subject,
        body,
        dst,
        sender,
        password,
        attachment,
        priority,
        cc,
        bcc,
        server,
        port,
    )


def sendthrottle(
        subject,
        body,
        dest,
        sender=smtp.SMTPUSERNAME,
        password=smtp.SMTPPASSWORD,
        attachment=None,
        priority=False,
        cc="",
        bcc="",
        server=smtp.SMTPSERVER,
):
    global threshold
    if env == constants.PRODUCTION:
        if threshold < 6:
            threshold += 1
            spawn(
                email,
                subject,
                body,
                constants.EMAIL_TEAM,
                sender,
                password,
                attachment,
                priority,
                "",
                "",
                server,
            )
    else:
        pass
    return


def outbox(
        subject,
        body,
        dest,
        sender=smtp.SMTPUSERNAME,
        password=smtp.SMTPPASSWORD,
        attachment=None,
        priority=False,
        cc="",
        bcc="",
        server=smtp.SMTPSERVER,
):
    return email(
        subject, body, dest, sender, password, attachment, priority, "", "", server
    )


def email(
        subject,
        body,
        dest,
        sender=smtp.SMTPUSERNAME,
        password=smtp.SMTPPASSWORD,
        attachment=None,
        priority=False,
        cc="",
        bcc="",
        server=smtp.SMTPSERVER,
        port=587,
):
    # create html email
    Msg = MIMEMultipart()
    Msg["Subject"] = subject
    Msg["From"] = sender
    if priority:
        Msg["X-Priority"] = "2"
    if isinstance(dest, list):
        Msg["To"] = ", ".join(dest)
    else:
        Msg["To"] = dest
        dest = [d.strip() for d in dest.split(",")]
    if cc:
        if isinstance(cc, list):
            Msg["Cc"] = ", ".join(cc)
        else:
            Msg["Cc"] = cc
            cc = [d.strip() for d in cc.split(",")]
        dest.extend(cc)
    if bcc:
        if isinstance(bcc, list):
            Msg["Bcc"] = ", ".join(bcc)
        else:
            Msg["Bcc"] = bcc
            bcc = [d.strip() for d in bcc.split(",")]
        dest.extend(bcc)
    Msg.attach(MIMEText(body, "html"))
    if attachment:
        part = MIMEBase("application", "octet-stream")
        part.set_payload((attachment.file).read())
        encoders.encode_base64(part)
        part.add_header(
            "Content-Disposition",
            "attachment; filename= {}".format(attachment.filename),
        )
        Msg.attach(part)
    status = True
    if "gmail.com" in server:
        conn = smtplib.SMTP_SSL(server, port)
        try:
            conn.set_debuglevel(1)
            conn.login(sender, password)
            conn.sendmail(sender, dest, Msg.as_string())
        except:
            status = False
            traceback.print_exc()
        finally:
            conn.quit()
    else:
        conn = smtplib.SMTP(server, port)
        try:
            conn.connect(server, port)
            conn.set_debuglevel(False)
            conn.starttls()
            conn.login(sender, password)
            conn.send_message(Msg)
        except Exception as exc:
            traceback.print_exc()
            status = False
        finally:
            conn.quit()
    return status


class SSLSettings(UserDict):
    def __init__(
            self,
            keyfile=None,
            certfile=None,
            ssl_version="PROTOCOL_SSLv23",
            ca_certs=None,
            do_handshake_on_connect=True,
            cert_reqs=CERT_NONE,
            suppress_ragged_eofs=True,
            ciphers=None,
            **kwargs,
    ):
        """settings of SSL
        :param keyfile: SSL key file path usally end with ".key"
        :param certfile: SSL cert file path usally end with ".crt"
        """
        UserDict.__init__(self)
        self.data.update(
            dict(
                keyfile=keyfile,
                certfile=certfile,
                server_side=True,
                ssl_version=getattr(ssl, ssl_version, ssl.PROTOCOL_SSLv23),
                ca_certs=ca_certs,
                do_handshake_on_connect=do_handshake_on_connect,
                cert_reqs=cert_reqs,
                suppress_ragged_eofs=suppress_ragged_eofs,
                ciphers=ciphers,
            )
        )


class EmailTemplates:
    @staticmethod
    def template(templatename, pl):
        templates = {
            "unsubscribe": (EmailTemplates.unsubscribe, "clientid", "customer"),
            "onboard": (EmailTemplates.onboard, "clientid", "brand"),
            "rejected": (EmailTemplates.rejected, "dba_name", "client_email", "brand"),
            "signup": (
                EmailTemplates.signup,
                "dba_name",
                "client_email",
                "message",
                "brand",
                "clientid",
            ),
            "alert": (EmailTemplates.alert, "type", "message", "clientname"),
        }
        if templatename in templates:
            code, *v = templates[templatename]
            args = tuple([pl.get(x) for x in v])
            return code(*args)
        return False


def callback(url, meta, headers=None, mode="POST"):
    header = {
        "Content-Type": constants.APPLICATION_JSON,
        "Accept": constants.APPLICATION_JSON,
    }
    header.update(dc(headers or {}))
    r = upool.request(mode, url, body=jc(meta), headers=header, retries=3)
    return r.data.decode()


def SMS(phone_number, message):
    pass


def detect_phone(phone_number):
    """
    Given a provided input, see if it is a phone number.
    """
    p = pparse(phone_number)

    try:
        phone_number.is_valid_number()
    except AttributeError:
        """
        This means we ran into an invalid phone number and should return as such.
        """
        return {"valid": False, "provider": None}

    return {
        "valid": phone_number.is_valid_number(),
        "provider": carrier.name_for_number(p, "en"),
    }


def do(address, message):  # in case we need a configuration that sends data
    if address:
        options = {"noreply": send, "callback": callback, "sms": SMS}
        mode = "callback"
        if "localhost" in address:
            mode = "callback"
        elif "https://" not in address and "@" in address:
            return options["noreply"](address, "TriplePlayPay Callback Email", message)
        elif "https://" not in address and "@" not in address:
            mode = "sms"
        return options[mode](address, message)