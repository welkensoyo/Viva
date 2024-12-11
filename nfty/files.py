import logging, traceback
import base64
import csv
import os
import bottle
import xlrd, openpyxl
from lxml import html
# from nfty.barcodes import BarcodePDF417
from io import StringIO, BytesIO
from PIL import Image
from nfty.docs import Document
from streamlit.runtime.uploaded_file_manager import UploadedFile

logger = logging.getLogger("AppLogger")

def parse_csv(file, delim=",", quotechar='"', binary=False):
    if binary:
        with StringIO(file.read().decode("utf-8")) as x:
            csvfile = csv.reader(x, delimiter=delim, quotechar=quotechar)
            for row in csvfile:
                yield row
    else:
        with open(file, "r") as f:
            csvfile = csv.reader(f, delimiter=delim, quotechar=quotechar)
            for row in csvfile:
                yield row


def write_csv(filename, data, header=None, delim=",", quotechar='"', binary=True):
    writemode = "w"
    if binary:
        writemode = "wb"
    with open(filename, writemode, encoding="utf-8") as f:
        csvfile = csv.writer(
            f,
            delimiter=delim,
            quotechar=quotechar,
            quoting=csv.QUOTE_MINIMAL,
            lineterminator="\n",
        )
        if header:
            csvfile.writerow(header)
        csvfile.writerows(data)
    return True

class Excel():
    def __init__(self, ext=None):
        self.file = None
        self.filename = None
        self.wb = None
        self.ext = ext
        self.delimiter = ''

    def open(self, file, data_only=False, filename=''):
        if filename:
            self.filename = os.path.basename(filename).replace(' ','_')
        if isinstance(file, UploadedFile):
            self.filename = file.name
            file = file.getvalue()
            binary = True
        elif isinstance(file, bottle.FileUpload):
            self.filename = file.filename
            file = file.file
            file.seek(0)
            binary = True
        elif isinstance(file, bytes):
            binary = True
        else:
            binary = False
            self.filename = os.path.basename(file).replace(' ','_')
        self.file = file
        if not self.ext:
            self.ext = os.path.splitext(self.filename)[1].lower()
        if self.ext in ('.csv','csv'):
            self._csv(binary=binary)
        elif self.ext in ('.xls','xls'):
            self._xls(binary=binary)
        elif self.ext in ('.xlsx','xlsx'):
            self._xlsx(binary=binary, data_only=data_only)
        return self

    def _xlsx(self, binary=False, data_only=False):
        if binary:
            try:
                self.wb = openpyxl.load_workbook(filename=BytesIO(self.file.read()), data_only=data_only)
            except:
                self.wb = openpyxl.load_workbook(filename=BytesIO(self.file), data_only=data_only)
        if not self.wb:
            self.wb = openpyxl.load_workbook(filename=self.file, data_only=data_only)
        if self.wb:
            self.ext = '.xlsx'

    def _xls(self, binary=False):
        if binary:
            try:
                self.wb = xlrd.open_workbook(file_contents=self.file)
            except:
                self.wb = False
        if not self.wb:
            try:
                self.wb = xlrd.open_workbook(self.file)
            except:
                self.wb = False
        if not self.wb:
            try:
                self.wb = open(self.file, 'r').read()
                self.ext = '.html'
            except:
                self.delimiter = '\t'
                return self._csv(binary=binary)
        elif self.wb:
            self.ext = '.xls'
        else:
            self.ext = '.xlsx'

    def _csv(self, binary=False):
        if binary:
            try:
                try:
                    self.wb = StringIO(self.file.decode())
                except:
                    self.wb = StringIO(self.file)
            except (UnicodeDecodeError, TypeError, AttributeError):
                self.ext = '.xls'
                return self
        if self.wb:
            self.ext = '.csv'

    def list_sheets(self):
        if self.wb is None:
            raise ValueError("Workbook is not loaded.")
        return self.wb.sheetnames

    def ws(self, sheetname=False, header=True, check_field=None, delimiter=','):
        if not self.delimiter:
            self.delimiter = delimiter
        if self.ext == '.html':
            tree = html.fromstring(self.wb)
            tables = tree.xpath('//table')
            ws = []
            longest = 0
            index = 0
            for i, table in enumerate(tables):
                rows = table.xpath('.//tr')
                data = [row for row in [[td.text_content() for td in row.xpath('.//th')] for row in rows] if len(row) > 1]
                data.extend([row for row in [[td.text_content() for td in row.xpath('.//td')] for row in rows] if len(row) > 1])
                ws.append(data)
                x = len(data)
                if x > longest:
                    longest = x
                    index = i
            return ws[index]
        if self.ext == '.csv':
            if self.wb:
                return self.csv_read_wb(header=header, delimiter=self.delimiter)
            return self.csv_reader(header=header, delimiter=self.delimiter)
        if self.ext == '.xls':
            if sheetname:
                ws = self.wb.get_sheet_by_name(sheetname)
            else:
                ws = self.wb.sheet_by_index(0)
            return [ws.row_values(row) for row in range(ws.nrows)]

        if not sheetname:
            ws = self.wb[self.wb.sheetnames[0]]
        else:
            try:
                if isinstance(sheetname, int):
                    ws = self.wb[self.wb.sheetnames[sheetname]]
                else:
                    ws = self.wb[sheetname]
            except KeyError: #in case the sheet doesn't exist try the first one
                ws = self.wb[self.wb.sheetnames[0]]
        xlist = []
        xa = xlist.append
        for row in ws.rows:
            xcell = []
            for cell in row:
                xcell.append(cell.value)
            if check_field:
                if xcell[check_field]:
                    xa(xcell)
            elif set(xcell) != set([None]):
                xa(xcell)
        if header == False:
            return xlist[1:]
        return xlist

    def ws_hyperlinks(self, sheetname=False, header=True):
        if not sheetname:
            ws = self.wb[self.wb.sheetnames[0]]
        else:
            ws = self.wb[sheetname]
        xlist = []
        xa = xlist.append
        for row in ws.rows:
            xcell = []
            for cell in row:
                try:
                    xcell.append(cell.hyperlink.target)
                except:
                    xcell.append(cell.value)
            xa(xcell)
        if header == False:
            return xlist[1:]
        return xlist

    def csv_gen(self, header=True):
        with open(self.file, 'r') as file:
            reader = csv.reader(file)
            if not header:
                next(reader)  # this will skip the header row
            for row in reader:
                yield row

    def csv_reader(self, header=True, check_field=False, delimiter=','):
        with open(self.file, 'r') as file:
            reader = csv.reader(file, delimiter=delimiter)
            if header == False:
                next(reader)  # this will skip the header row
            if check_field:
                return [row for row in reader if row[check_field]]
            else:
                return [row for row in reader]

    def csv_read_wb(self, header=True, check_field=False, delimiter=','):
        reader = csv.reader(self.wb, delimiter=delimiter)
        if header == False:
            next(reader)  # this will skip the header row
        if check_field:
            return [row for row in reader if row[check_field]]
        else:
            return [row for row in reader]

    def header_map(self, header):
        x = {}
        for i, h in enumerate(header):
            if h:
                x[h.lower().strip()] = i
        return x


class FileUpload:
    def __init__(self, request, client):
        self.c = client
        self.request = request
        self.files = request.files
        self.header = []
        self.columns = {}
        self.filename = None
        self.file = None
        self.count = 0
        self.response = {}

    # def license(self):
    #     for self.filename, self.file in list(self.files.items()):
    #         if x := BarcodePDF417(self.file, save=True).detect():
    #             return x
    #     return {}

    def process(self):
        for self.filename, self.file in list(self.files.items()):
            self.detect()
        return self.response

    # def onboard(self, ws):
    #     """
    #     This takes in an Excel file and with the parent id values, loads users into the system.
    #     """
    #     parentids = ("id", "clientid", "parentid", "parent")
    #     parent = constants.CLIENT_ID_DEFAULT_API
    #     for line in ws:
    #         meta = {self.columns[i]: v for i, v in enumerate(line) if v}
    #         for p in parentids:
    #             if meta.get(p):
    #                 parent = meta.pop(p)
    #                 break
    #         if not parent:
    #             parent = self.c.id
    #         if meta:
    #             self.count += 1
    #             e = Enroll()
    #             e.post(meta, parent=parent, skip=True)
    #             e.set_status(status=constants.ONBOARDING_SEND_LINK)
    #     self.response = f"Successfully imported {self.count} clients"
    #     return self

    def detect(self):
        if self.filename == "file":
            self.response = Document(self.request, self.c).new()
            return self
        name, ext = self.filename.rsplit(".", 1)
        # if ext in ("xls", "xlsx"):
        #     ws = Excel(self.file.file).wsgen()
        #     header = ws.__next__()
        #     if len(fileTypes["enroll"].intersection(set(header))) == 6:
        #         self.columns = {i: v.lower() for i, v in enumerate(header)}
        #         self.onboard(ws)
        #     elif len(fileTypes["enroll_2"].intersection(set(header))) == 6:
        #         rename = fileTypes["enroll_map"]
        #         header = [rename.get(h, h) for h in header]
        #         self.columns = {i: v.lower() for i, v in enumerate(header)}
        #         self.onboard(ws)
        # else:
        self.response = Document(self.request, self.c).upload(self.file)
        return self

    def logo(self):
        def resize_image(image, dimensions):
            i = Image.open(image.file)
            i.thumbnail(dimensions)
            return i

        imagedict = {}
        name, ext = os.path.splitext(self.filename)
        image = resize_image(self.file, (400, 400))
        buffer = BytesIO()
        image.save(buffer, optimize=True, format="PNG")
        imagedict[name + ".PNG"] = base64.b64encode(buffer.getvalue()).decode()
        self.response.update(imagedict)
        return self



def delete_file(file_path):
    try:
        os.remove(file_path)
    except:
        pass

