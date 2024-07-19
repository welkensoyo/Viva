import base64
import math
from io import BytesIO
import logging
import qrcode
import requests
import xlsxwriter
from PIL import Image
from lxml import etree
from api._config import appconfig


logger = logging.getLogger("AppLogger")


class DummyObject(object):
    def __getattr__(self, name):
        return lambda *x: None


# sv likes to puke the bitmap of the image into the log, which gets rediculous. So let's turn it off.
# sv.logger = DummyObject()


class QR:
    def __init__(self):
        logger.info("QR __INIT__ Called")
        self.block_size = 10
        self.circle_radius = 20 * 4
        self.color = ["black", "white"]

    def createqr(self, url, **kwargs):
        qr = qrcode.QRCode(kwargs)
        qr.add_data(url)
        qr.make(fit=True)
        img = qr.make_image(
            fill_color=self.color[0], back_color=self.color[1], mode="RGBA"
        )
        return img

    def create(self, logopath, url, logosize=None):
        if len(url) <= 25:
            logosize = logosize or 150
            ec = qrcode.constants.ERROR_CORRECT_H
        else:
            logosize = logosize or 300
            ec = qrcode.constants.ERROR_CORRECT_Q
        qr = qrcode.QRCode(error_correction=ec, border=4)
        qr.add_data(url)
        qr.make(fit=False)
        img = qr.make_image(
            fill_color=self.color[0], back_color=self.color[1], mode="RGBA"
        )
        img = img.convert("RGBA")
        width, height = img.size
        if logopath:
            if "https" in logopath:
                dl = requests.get(logopath)
                logo = Image.open(BytesIO(dl.content))
            elif (
                logopath[0:7] == "static/" or logopath[0:8] == "storage/"
            ):  # local image like 'static/images/LogoIcon_Stroke.png'
                logo = Image.open(logopath)
            else:
                logo = base64.urlsafe_b64decode(logopath.encode())
                logo = Image.open(BytesIO(logo))
            logo = logo.convert("RGBA")
            xmin = ymin = int((width / 2) - (logosize / 2))
            xmax = ymax = int((width / 2) + (logosize / 2))
            logo = logo.resize((xmax - xmin, ymax - ymin))
            img.paste(logo, (xmin, ymin, xmax, ymax), mask=logo)
        return img

    # no longer used
    def createsvg(self, logopath, url, outputfile):
        def touchesBounds(center, x, y, radius, block_size):
            def distance(p0, p1):
                return math.sqrt((p0[0] - p1[0]) ** 2 + (p0[1] - p1[1]) ** 2)

            scaled_center = center / block_size
            dis = distance((scaled_center, scaled_center), (x, y))
            rad = radius / block_size
            return dis <= rad + 0

        self.logoPath = logopath
        self.url = url
        self.outputname = outputfile
        if len(url) <= 25:
            ec = qrcode.constants.ERROR_CORRECT_H
        else:
            ec = qrcode.constants.ERROR_CORRECT_Q
        img = self.createqr(self.url, error_correction=ec, box_size=1, border=4)
        imageSize = str(img.size[0] * self.block_size)
        doc = etree.Element(
            "svg",
            width=imageSize,
            height=imageSize,
            version="1.1",
            xmlns="http://www.w3.org/2000/svg",
        )
        pix = img.load()
        center = img.size[0] * self.block_size / 2
        for xPos in range(0, img.size[0]):
            for yPos in range(0, img.size[1]):
                color = pix[xPos, yPos]
                if color == 0:
                    withinBounds = not touchesBounds(
                        center, xPos, yPos, self.circle_radius, self.block_size
                    )
                    if withinBounds:
                        etree.SubElement(
                            doc,
                            "rect",
                            x=str(xPos * self.block_size),
                            y=str(yPos * self.block_size),
                            width="10",
                            height="10",
                            fill="black",
                        )

        document = etree.parse(self.logoPath)
        logo = document.xpath('//*[local-name()="svg"]')[0]

        width = float(str(logo.get("width")).replace("px", ""))
        height = float(str(logo.get("height")).replace("px", ""))
        scale = self.circle_radius * 2 / width
        scale_str = "scale(" + str(scale) + ")"
        xTrans = ((img.size[0] * self.block_size) - (width * scale)) / 2
        yTrans = ((img.size[1] * self.block_size) - (height * scale)) / 2

        translate = "translate(" + str(xTrans) + " " + str(yTrans) + ")"
        logo_scale_container = etree.SubElement(
            doc, "g", transform=translate + " " + scale_str
        )
        for element in logo.getchildren():
            logo_scale_container.append(element)

        # ElementTree 1.2 doesn't write the SVG file header errata, so do that manually
        f = open(self.outputname, "w")
        f.write('<?xml version="1.0" standalone="no"?>\n')
        f.write('<!DOCTYPE svg PUBLIC "-//W3C//DTD SVG 1.1//EN"\n')
        f.write('"https://www.w3.org/Graphics/SVG/1.1/DTD/svg11.dtd">\n')
        f.write(etree.tostring(doc))
        f.close()


def make64(text, color="", center="", receipt=False):
    qr = QR()
    if color == "white":
        qr.color = ["white", "black"]
    elif color == "nfty":
        qr.color = ["#e94601", "white"]
    elif color == "black":
        qr.color = ["black", "white"]
    elif color and "," in color:
        color = color.split(",")
        qr.color = ["#" + str(color[0]), "#" + str(color[1])]
    # center = 'static/images/LogoIcon_Stroke.png'
    img = qr.create(center, text, logosize=128)
    if receipt:
        return img
    b = BytesIO()
    img.save(b, "png", optimize=True)
    # img.show()
    return base64.b64encode(b.getvalue())


def makefiles(zipcode, clienthash, count, togo=True, route="app"):
    xlist = []
    baseurl = appconfig["rootserver"]
    savepath = appconfig["qrsavepath"]
    workingpath = savepath[:-4].format(zipcode, clienthash)
    if not os.path.exists(workingpath):
        os.makedirs(workingpath)
    elif len(os.listdir(workingpath)) == int(count) + 1:
        for x in range(int(count) + 1):
            if x == 0:
                if togo == False:
                    continue
                posid = "togo"
            else:
                posid = x
            url = baseurl + "/{3}/{0}/{1}/{2}".format(zipcode, clienthash, posid, route)
            filename = "{0}-{1}-{2}.png".format(zipcode, clienthash, posid)
            imgurl = baseurl + "/pics/qrcode/{0}/{1}/{2}".format(
                zipcode, clienthash, filename
            )
            xlist.append([url, posid, imgurl])
        return xlist
    else:
        for f in [f for f in os.listdir(workingpath)]:
            os.remove(os.path.join(workingpath, f))
    filex = workingpath + "/nftyqrcodes.xlsx"
    workbook = xlsxwriter.Workbook(filex)
    worksheet = workbook.add_worksheet()
    format = workbook.add_format()
    format.set_align("center")
    format.set_align("vcenter")
    worksheet.set_column("A:A", 40)
    worksheet.set_column("B:B", 5)
    worksheet.set_column("C:C", 25)
    row = 0
    col = 0
    qr = QR()
    for x in range(int(count) + 1):
        finalpng = BytesIO()
        if x == 0:
            if togo == False:
                continue
            posid = "togo"
        else:
            posid = x
        filename = "{0}-{1}-{2}.png".format(zipcode, clienthash, posid)
        completepath = savepath.format(zipcode, clienthash, filename)
        imgurl = baseurl + "/pics/qrcode/{0}/{1}/{2}".format(
            zipcode, clienthash, filename
        )
        url = baseurl + "/{3}/{0}/{1}/{2}".format(zipcode, clienthash, posid, route)
        background = qr.create("static/images/LogoIcon_Stroke.png", url)
        background.save(completepath, "PNG")
        xlist.append([url, posid, imgurl])
        worksheet.write(row, col, url, format)
        worksheet.write(row, col + 1, posid, format)
        worksheet.set_row(row, 90)
        background.save(finalpng, "PNG")
        worksheet.insert_image(
            row,
            col + 2,
            filename,
            {"x_scale": 0.25, "y_scale": 0.25, "image_data": finalpng},
        )
        row += 1
    workbook.close()
    return xlist


if __name__ == "__main__":
    import os

    os.chdir("../")
    print(os.listdir())
    print(
        make64(
            "https://dsjkfhdkfjhadfhjasdfkljhasdlfkjhadlfjkhasdfjlkhasdfljkhasdfjlkhadsl",
            center="static/images/coins/32/color/eth.png",
        )
    )
