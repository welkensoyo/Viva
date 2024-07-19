from gevent import spawn
from PIL import Image
from PIL import ImageFilter, ImageEnhance
import re
from pyzbar.pyzbar import decode as zbdecode, ZBarSymbol
import zxingcpp as zxing
import numpy as np
import cv2
from uuid import uuid4
from io import BytesIO

class BarcodePDF417:

    LICENSE_CODES = {
    'DCB': 'restrictions',
    'DCD': 'endorsement',
    'DBA': 'expiration',
    'DCS': 'last_name',
    'DCT': 'first_name',
    'DBD': 'license_date',
    'DBB': 'dob',
    'DBC': 'sex',  # 1 for male, 2 for female
    'DAY': 'eye_color',
    'DAU': 'height_full',
    'DAG': 'address',
    'DAI': 'city',
    'DAJ': 'state',
    'DAK': 'zipcode_full',
    'DAQ': 'drivers_license_number',
    'DCF': 'document_discriminator',
    'DCG': 'country',
    'DCK': 'inventory_control_number',
}

    def __init__(self, image, save=False):
        if save:
            self.save_image(image)
        self.original = image
        self.image = None
        self.barcode_data = {}

    def _cv2(self):
        try:
            self.original.file.seek(0)
            self.image = cv2.imdecode(np.frombuffer(self.original.file.read(), np.uint8), cv2.IMREAD_COLOR)
        except AttributeError:
            self.image = cv2.imread(self.original, cv2.IMREAD_COLOR)
        return self

    def _cv2_gray(self):
        try:
            self.original.file.seek(0)
            self.image = cv2.imdecode(np.frombuffer(self.original.file.read(), np.uint8), cv2.IMREAD_GRAYSCALE)
        except AttributeError:
            self.image = cv2.imread(self.original, cv2.IMREAD_GRAYSCALE)
        return self

    def _cv2_pil(self):
        self.image = np.array(self.image)
        return self

    def _pil(self):
        try:
            self.original.file.seek(0)
            self.image = Image.open(BytesIO(self.original.file.read())).convert('L')
        except AttributeError:
            self.image = Image.open(self.original).convert('L')
        return self

    def zbar(self, image):
        for i, barcode in zbdecode(image, symbols=[ZBarSymbol.PDF417]):
            return self.decode_license(barcode)
        return False

    def show(self, image):
        def _(img):
            if isinstance(img, np.ndarray):
                cv2.imshow('image', img)
                cv2.waitKey(0)
                cv2.destroyAllWindows()
            else:
                img.show()
        spawn(_, image)

    def pre_process(self):
        bc = cv2.cvtColor(self.image, cv2.COLOR_BGR2GRAY) if len(self.image.shape) == 3 else self.image
        _, bc = cv2.threshold(bc, 90, 255, cv2.THRESH_BINARY | cv2.THRESH_OTSU)
        return bc

    def pre_process_adaptive(self, block, threshold):
        bc = cv2.cvtColor(self.image, cv2.COLOR_BGR2GRAY) if len(self.image.shape) == 3 else self.image
        bc = cv2.adaptiveThreshold(bc, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, block, threshold)
        return bc

    def pixilate(self, image, pixel_size):
        # Convert pixel_size to integer
        pixel_size = int(pixel_size)
        # Scale down the image to small size
        small = cv2.resize(image, (0, 0), fx=1.0 / pixel_size, fy=1.0 / pixel_size, interpolation=cv2.INTER_NEAREST)

        # Scale up the small image to original size
        pixelated = cv2.resize(small, image.shape[:2][::-1], interpolation=cv2.INTER_NEAREST)
        return pixelated

    def pre_process_pil(self, sharpness=2.0, contrast=2.0, brightness=0.0, saturation=1.0):
        img = self.image
        # img = img.rotate(-90)
        if brightness:
            img = ImageEnhance.Brightness(img).enhance(brightness)
        if sharpness:
            img = ImageEnhance.Sharpness(img).enhance(sharpness)
        if contrast:
            img = ImageEnhance.Contrast(img).enhance(contrast)
        return img

    def sharpen_image(self):
        blurred = cv2.GaussianBlur(self.image, (0, 0), 3)
        self.image = cv2.addWeighted(self.image, 1.5, blurred, -0.5, 0)
        return self.image

    def save_image(self, file):
        print('saving...')
        file.save(f'/Users/derekbartron/Pictures/dumpsterfire/scans/test_{uuid4()}.png')

    def decode_license(self, text):
        text = text.split('<LF>')
        self.barcode_data = {self.LICENSE_CODES[line[0:3]]: line[3:].strip() for line in text if self.LICENSE_CODES.get(line[0:3])}
        for value in ('dob', 'license_date', 'expiration'):
            if value in self.barcode_data:
                self.barcode_data[value] = f"{self.barcode_data[value][4:8]}-{self.barcode_data[value][0:2]}-{self.barcode_data[value][2:4]}"
        for value in ('first_name', 'last_name', 'city'):
            if value in self.barcode_data:
                self.barcode_data[value] = self.barcode_data[value].title()
        if 'sex' in self.barcode_data:
            self.barcode_data['sex'] = 'male' if self.barcode_data['sex'] == '1' else 'female'
        if self.barcode_data.get('country') == 'USA':
            self.barcode_data['zipcode'] = self.barcode_data['zipcode_full'][0:5]
        if 'in' in self.barcode_data.get('height_full', ''):
            height = int(re.findall(r'\d+', self.barcode_data['height_full'])[0])/12
            self.barcode_data['height'] = f"{height:.2f}"
        elif 'cm' in self.barcode_data.get('height_full', ''):
            self.barcode_data['height'] = str(int(re.findall(r'\d+', self.barcode_data['height_full'])[0]))
        return self.barcode_data

    def zxing_detect(self, processed_image):
        for each in zxing.read_barcodes(processed_image, formats=zxing.BarcodeFormat.PDF417):
            if each.format == zxing.BarcodeFormat.PDF417:
                return self.decode_license(each.text)
        return False

    def detect(self):
        for func in (self.detect_pil, self.detect_cv2_pil_sharpen_contrast, self.detect_cv2_sharpen, self.detect_cv2, self.detect_raw, self.detect_zbar):
            if _ := func():
                print(f'Found {func.__name__}')
                return _
        return False

    def detect_pil(self):
        self._pil()
        if _ := self.zxing_detect(self.image):
            return _
        for sharpness in (0.5, 1, 2, 1.5):
            for contrast in (0.5, 1, 2, 1.5):
                if _ := self.zxing_detect(self.pre_process_pil(sharpness=sharpness, contrast=contrast)):
                    return _
        return False

    def detect_cv2(self):
        self._cv2_gray()
        if _ := self.zxing_detect(self.pre_process()):
            return _
        for block, threshold in ((27, 5), (27, 1), (17, 1)):
            if _ := self.zxing_detect(self.pre_process_adaptive(block, threshold)):
                return _
        return False

    def detect_cv2_adaptive(self):
        self._cv2_gray()
        for x in (17, 27, 207):
            for y in range(1, 5):
                try:
                    if _ := self.zxing_detect(self.pre_process_adaptive(x, y)):
                        return _
                except:
                    continue

    def detect_cv2_sharpen(self):
        self._cv2_gray()
        self.sharpen_image()
        for x in (17, 27, 207):
            for y in range(1, 5):
                try:
                    if _ := self.zxing_detect(self.pre_process_adaptive(x, y)):
                        return _
                except:
                    continue

    def detect_cv2_pil_sharpen_contrast(self):
        for sharpness in (0.5, 1, 2, 1.5):
            for contrast in (0.5, 1, 2, 1.5):
                self._pil()
                self.image = self.pre_process_pil(sharpness=sharpness, contrast=contrast)
                self._cv2_pil()
                if _ := self.zxing_detect(self.pre_process_adaptive(207, 2)):
                    print(sharpness, contrast)
                    return _

    def detect_hardcore(self):
        for sharpness in (0.5, 1, 2, 3,  1.5):
            for contrast in (0.5, 1, 2, 3, 1.5):
                self._pil()
                self.image = self.pre_process_pil(sharpness=sharpness, contrast=contrast)
                self._cv2_pil()
                for x in range(11, 207):
                    for y in range(1, 9):
                        try:
                            if _ := self.zxing_detect(self.pre_process_adaptive(x, y)):
                                print(sharpness, contrast, x, y)
                                return _
                        except:
                            continue

        # if _ := self.zxing_detect(self.pre_process_adaptive(17, 2)):
        #     return _

    def detect_zbar(self):
        self._pil()
        if _ := self.zbar(self.image):
            return _

    def detect_raw(self):
        self._pil()
        if _ := self.zxing_detect(self.image):
            return _
        return {}

    def detect_test(self):
        self._pil()
        self.image = self.pre_process_pil(sharpness=0, contrast=0)
        self._cv2_pil()
        if _ := self.zxing_detect(self.pre_process_adaptive(27, 3)):
            return _




def scan_dir(directory='/Users/derekbartron/Pictures/dumpsterfire/scans/'):
    for file in os.listdir(directory):
        if '.png' in file or 'jpeg' in file:
            print(file)
            if _ := BarcodePDF417(directory + file, save=False).detect():
                print(_)

def scan_file_test(file, scan_dir='/Users/derekbartron/Pictures/dumpsterfire/scans/'):
    # print(BarcodePDF417(scan_dir + file, save=False).detect_zbar())
    return BarcodePDF417(scan_dir + file, save=False).detect_test()

def scan_file(file, scan_dir='/Users/derekbartron/Pictures/dumpsterfire/scans/'):
    # print(BarcodePDF417(scan_dir + file, save=False).detect_zbar())
    return BarcodePDF417(scan_dir + file, save=False).detect()

if __name__ == "__main__":
    import os
    directory = '/Users/derekbartron/Pictures/dumpsterfire/scans/'
    file = 'test_d06405a5-855e-4297-acbf-f71000ea8b2d.png'
    file = '20230922_154545.png'
    # file = 'test_cropped.png'
    # print(scan_file_test(file))
    print(scan_dir(directory=directory))
    # x = BarcodePDF417(scan_dir + file, save=False).detect_cv2_adaptive()
    # print(x)

    # file = "/Users/derekbartron/Pictures/dumpsterfire/scans/test_88f10c1b-3e9f-40d9-bb79-d9b173546388.png"
    # strip_black(file)
    # file = "/Users/derekbartron/Pictures/dumpsterfire/camera_bc.png"
    # file = "/Users/derekbartron/Pictures/dumpsterfire/pdf417djb.jpeg"
    # d = detect_barcode(file)
    # print(d)
    # print(PDF417Decoder(file))
    # print(zbar_pdf417(file))

