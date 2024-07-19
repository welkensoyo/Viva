import base64
from io import BytesIO
import logging
import arrow
import requests
from PIL import ImageFont, Image, ImageDraw
import nfty.db as db
from api.clients import Client
from api._config import systemoptions
from nfty.njson import jc
from nfty.qr import make64 as QR


logger = logging.getLogger("AppLogger")

trans_table = systemoptions["transaction"]

qry = {
    "id": """ SELECT clientid, meta-'request'-'_bin', created FROM nfty.transactions WHERE meta @> %s  """
}


class CreateError(Exception):
    """This is a base class for all CREATE related exceptions"""

    pass


class Generator:
    FONTS = {
        "": ("ptmono.ttf", 14),
        "anonymous": ("Anonymous.ttf", 12),
        "droid": ("DroidSansMono.ttf", 14),
        "space": ("SpaceMono-Regular.ttf", 13),
        "vera": ("VeraMono.ttf", 14),
        "free": ("FreeMono.ttf", 14),
        "freebold": ("FreeMonoBold.ttf", 14),
        "pt": ("ptmono.ttf", 14),
    }

    def __init__(self, id, qr=True):
        self.id = id
        self.binary = None
        self.qr = qr
        self.clientid, self.order, self.created = db.fetchone(
            qry["id"], jc({"id": self.id})
        ) or (None, {}, None)
        self.c = Client(self.clientid)
        self.head = self.c.name
        self.output = "link"  # or 'base64'
        self.font = "droid"
        self.width = 420
        self.fontsize = 14
        self.leftpadding = 10
        self.rightpadding = 10
        self.tax = self.order.get("tax", 0)
        self.tip = self.order.get("tip", 0)
        self.signature = False

    def fake(self):
        return [
            {
                "id": "",
                "price": "12.00",
                "description": "Line Item1",
                "tax": 1.00,
                "options": "carrots",
            },
            {
                "id": "",
                "price": 15.00,
                "description": "Line Item2",
                "tax": "1.00",
                "options": ["carrots", "fries"],
            },
            {
                "id": "",
                "price": 8.0,
                "description": "Line Item3",
                "tax": "1.00",
                "options": ["fries"],
            },
            {
                "id": "",
                "price": 9,
                "description": "Line Item4",
                "tax": 1,
                "options": "fries, carrots, onion rings",
            },
        ]

    def _f(self, test):
        return f"{float(test):.2f}"

    def _qr(self):
        return QR(
            f"https://tripleplaypay.com/receipt/{self.id}",
            "static/images/LogoIcon_Stroke.png",
            receipt=True,
        )

    def setup(self, meta):
        self.font, self.fontsize = self.FONTS.get(meta.get("font", self.font), "")
        self.width = int(meta.get("width", self.width))
        self.output = meta.get("output", self.output)
        self.signature = (
            True
            if meta.get("signature") not in (False, "False", "false", "0")
            else False
        )
        img = meta.get("image")
        txt = meta.get("text", self.head)
        if img:
            if "https" in img:
                dl = requests.get(img)
                self.head = Image.open(BytesIO(dl.content))
            elif (
                img[0:7] == "static/" or img[0:8] == "storage/"
            ):  # local image like 'static/images/LogoIcon_Stroke.png'
                self.head = Image.open(img)
            else:
                logo = base64.urlsafe_b64decode(img.encode())
                self.head = Image.open(BytesIO(logo))
        elif txt:
            if isinstance(txt, (list, tuple)):
                self.head = "\n" + "\n".join(txt) + "\n\n"
            else:
                self.head = "\n" + txt + "\n\n"

    def body(self):
        textlist = []
        tappend = textlist.append
        textbody = f"{self.c.name}".center(50)
        if self.c.config.get("whitelabel"):
            pass  # add whitelabel details
        textbody += "\n"
        textbody += (
            f"{arrow.get(self.created).format('YYYY-MM-DD HH:mm').center(50)}\n\n"
        )
        textbody += "\n\n"
        textbody += f"Ticket ID:"
        textbody += "\n"
        textbody += f"{self.id:>40}"
        textbody += "\n"
        textbody += f"Store ID:"
        textbody += "\n"
        textbody += f"{self.c.hashcode:>40}"
        textbody += "\n"
        if "details" in self.order:
            if "Card" in self.order["details"]:
                logo = self.order["details"]["Card"].get("CardLogo")
                card = self.order["details"]["Card"].get("CardNumberMasked")
                card = logo + "   " + card
                textbody += "\n"
                textbody += f"Card     {card:>40}"
                textbody += "\n"
        width = 0
        tax = 0.0
        for item in self.order.get("receipt", ()):  # or self.fake():
            tax += float(item.get("tax", 0))  # in case we want to calculate later
            text = "${0:.20}  {1}\n".format(self._f(item["price"]), item["description"])
            tappend(text)
            if len(text) > (width):
                width = len(text)
        if not self.tax:
            self.tax = tax
        for text in textlist:
            textbody += "".join(text)
        textbody += "-" * (width * 2) + "\n"
        if self.tax:
            textbody += f"${self._f(self.tax)}  + TAX".rjust(42)
            textbody += "\n"
        if self.tip:
            textbody += f"${self._f(self.tip)}  + TIP".rjust(42)
            textbody += "\n"
        textbody += "\n"
        textbody += f"TOTAL = ${self._f(self.order['amount'])} ".rjust(42)
        textbody += "\n\n\n\n\n"
        if self.signature:
            textbody += "x_______________________________________________________\n\n"
            textbody += "I agree to pay the above total amount".center(48)
            textbody += "\n"
            textbody += "according to the card issuer agreement".center(48)
            textbody += "\n"
            textbody += "\n"
            textbody += "Customer Copy".center(48)
        else:
            textbody += "Merchant Copy".center(48)
        return textbody

    def generate(self, settings=None):
        if settings:
            self.setup(settings)
        if not self.order or len(self.order.keys()) < 3:
            return {"status": False, "message": "Receipt Not Found"}
        self.binary = self.image(self.body())
        if self.output == "link":
            return self.link()
        return self.png()

    # in_memory_file = s.getvalue()
    # raw_data_file = img.tobytes()
    def image(self, text, color="rgb(0,0,0)", bgcolor="rgb(255,255,255)"):
        qrh, qrw = 0, 0
        imqr = None
        if self.qr:
            imqr = self._qr().resize((round(self.width * 0.5), round(self.width * 0.5)))
            qrh, qrw = imqr.size
        font = ImageFont.truetype("static/fonts/" + self.font, self.fontsize)
        fontlinkback = ImageFont.truetype("static/fonts/receipt.ttf", 10)
        linkback = "\nPowered by tripleplaypay.com"
        linkback_height = fontlinkback.getmask(linkback).getbbox()[3]
        lines = text.split("\n")
        line = ""
        if len(line) != 0:
            lines.append(line[1:])  # add the last line
        line_height = font.getmask(text).getbbox()[3]
        img_height = line_height * (len(lines) + 1)
        img = Image.new("RGBA", (self.width, img_height + qrh + 20), bgcolor)
        draw = ImageDraw.Draw(img)
        y = 0
        for line in lines:
            line = (
                line or " "
            )  # added to fix a weird bug where if empty generated an error
            draw.text((self.leftpadding, y), line, color, font=font)
            y += line_height
        if imqr:
            img.paste(imqr, (100, img_height))
        draw.text(
            (self.leftpadding + 95, img_height + qrh - linkback_height),
            linkback,
            color,
            font=fontlinkback,
        )
        b = BytesIO()
        img.save(b, "PNG")
        # img.show()
        return base64.b64encode(b.getvalue()).decode()

    def link(self):
        return f"""<a href="" class="external" download="{self.c.name}_{self.id}.png" id="receipta"><img id="receipti" src="data:image/png;base64,{self.binary}"/></a> """

    def png(self):
        return self.binary


if __name__ == "__main__":
    import os

    os.chdir("../")
    cfg = {
        "text": "THANK YOU FOR SHOPPING AT WALMART\nWE APPRECIATE YOUR PATRONAGE\nREMEMBER TO THANK US ON YELP!",
        "size": 16,
        "width": 400,
        "font": "",
    }
    r = Generator("98918534-df64-4c68-940e-cf73d126fd27").generate(settings=cfg)
