import cv2
import pytesseract
from pytesseract import Output
from pprint import pprint
import arrow
from nfty.ziptime import states
import usaddress


class DriverLicenseScanner:
    example = {
        "first_name": '',
        "last_name": '',
        "middle_name": '',
        "birth_date": '',
        "height": '',
        "contact": {},
        "sex": ''}

    def __init__(self, image_path):
        self.image = cv2.imread(image_path, cv2.IMREAD_GRAYSCALE)
        self.data = {}
        self.text = []
        self.info = {}
        self.final = {}

    def whitebalance(self):
        wb = cv2.xphoto.createSimpleWB()
        self.image = wb.balanceWhite(self.image)
        return self

    def contrast(self, alpha=1.1, beta=20.0):
        self.image = cv2.convertScaleAbs(self.image, alpha=alpha, beta=beta)
        return self

    def sharpen_image(self):
        blurred = cv2.GaussianBlur(self.image, (0, 0), 3)
        self.image = cv2.addWeighted(self.image, 1.5, blurred, -0.5, 0)
        return self

    def check_for_state(self, list1):
        if state := set(list1) & set(states.keys()):
            end = list1.index(list(state)[0]) + 2
            try:
                a, _ = usaddress.tag(' '.join(list1[0:end]))
                if a:
                    self.parse_address(dict(a))
            except:
                pass
        return self

    def check_for_birthdate(self, date):
        x = None
        try:
            if '-' in date:
                x = arrow.get(date, 'MM-DD-YYYY')
            elif '/' in date:
                x = arrow.get(date, 'MM/DD/YYYY')
            if arrow.now().year - x.year >= 18:
                self.info["birth_date"] = x.format('YYYY-MM-DD')
        except:
            x = None
        if not x and '-' in date:
            x = date.split('-')
            if len(x) == 2:
                try:
                    if int(x[0]) < 9:
                        feet = x[0].strip()
                        inches = x[1].replace('"', '').strip()
                        self.info["height"] = f'{feet}-{inches}'
                except:
                    pass
        return self

    def parse_address(self, address):
        if n := address.pop('Recipient', '').split():
            self.info['last_name'] = n[0].capitalize()
            self.info['first_name'] = n[1].capitalize()
            if len(n) > 2:
                self.info['middle_name'] = ' '.join(n[2:])
        address_format = "{AddressNumber} {StreetNamePreDirectional} {StreetName} {StreetNamePostType} {StreetNamePostDirectional} {OccupancyType} {OccupancyIdentifier}, {PlaceName}, {StateName} {ZipCode}"
        address_str = address_format.format(**{k: address.get(k, '') for k in usaddress.LABELS})
        address_str = ' '.join(address_str.split())
        self.info['contact'] = {
            'full_address': address_str,
            'state': address.get('StateName'),
            'zipcode': address.get('ZipCode','')[0:5],
            'city': address.get('PlaceName','').capitalize(),
            'address': ' '.join([address.get('AddressNumber', ''), address.get('StreetName', ''), address.get('StreetNamePostType', ''), address.get('StreetNamePostDirectional', '')]).strip(),
            'address_2': ' '.join([address.get('OccupancyType', ''), address.get('OccupancyIdentifier', '')]).strip()
        }

    def convert_dict(self):
        data = {}
        for i in range(len(self.data['line_num'])):
            txt = self.data['text'][i]
            block_num = self.data['block_num'][i]
            line_num = self.data['line_num'][i]
            # top, left = self.data['top'][i], self.data['left'][i]
            # width, height = self.data['width'][i], self.data['height'][i]
            if not (txt == '' or txt.isspace()):
                # tup = (txt, left, top, width, height)
                if block_num in data:
                    if line_num in data[block_num]:
                        data[block_num][line_num].append(txt)
                    else:
                        data[block_num][line_num] = [txt]
                else:
                    data[block_num] = {}
                    data[block_num][line_num] = [txt]
        # bounding box data
        # linedata = {}
        # idx = 0
        # for _, b in data.items():
        #     for _, l in b.items():
        #         linedata[idx] = l
        #         idx += 1
        # line_idx = 1
        # for _, line in linedata.items():
        #     xmin, ymin = line[0][1], line[0][2]
        #     xmax, ymax = (line[-1][1] + line[-1][3]), (line[-1][2] + line[-1][4])
        #     line_idx += 1
        return data

    def scan_license(self):
        self.data = pytesseract.image_to_data(self.image, output_type=Output.DICT)
        self.data = self.convert_dict()
        return self.text_search()

    def text_search(self):
        for block in self.data:
            line = []
            [line.extend(self.data[block][group]) for group in self.data[block]]
            line = [l for l in line if l and any(char.isalnum() for char in l)]
            self.check_for_state(line)
            for item in line:
                if '"' in item and any(char.isdigit() for char in item):
                    self.info['height'] = item
                self.check_for_birthdate(item)
                if item.upper().strip() == 'M':
                    self.info['sex'] = 'M'
                elif item.upper().strip() == 'F':
                    self.info['sex'] = 'F'
        return self

    def apply_operations(self, op):
        if op:
            getattr(self, op)()
        return self

    def detect(self):
        operations = ('', 'whitebalance', 'sharpen_image', 'contrast')
        for ops in operations:
            self.apply_operations(ops).scan_license()
            print(len(self.info.values()))
            print(len(self.final.values()))
            if len(self.info.values()) > len(self.final.values()):
                self.final = self.info.copy()
            # If we have enough values, we can exit early
            if len(self.info.values()) >= 7:
                break
        return self



if __name__ == "__main__":
    directory = '/Users/derekbartron/Pictures/dumpsterfire/front/'
    # file = 'test_d573e042-9667-423d-8b52-95b3616a5a16.png'
    # file = 'generic-drivers-license.jpg'
    file = "REAL ID-Compliant Non-Commercial Driver's License - Mid-Renewal Cycle.jpg"
    d = DriverLicenseScanner(directory+file).detect()
    pprint(d.final)