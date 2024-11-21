import traceback
from nfty.files import Excel
from pprint import pprint
from datetime import datetime
import re, arrow
import nfty.db as db
import nfty.njson as json

qry = {
    'get' : ''' SELECT meta FROM objects.cache WHERE id = %s ''',
    'upsert' : ''' INSERT INTO objects.cache (id, meta) VALUES (%s, %s) ON CONFLICT (id) DO UPDATE SET meta = EXCLUDED.meta; ''',
}



class ProcessFile:
    def __init__(self, file):
        self.file = file
        self.meta = {}
        self.file_name = ''
        self.sites = {'ft.':'Ft Worth', 'austin':'Austin','dallas':'Richardson','richardson':'Richardson'}

    def get_meta(self, id='ROLLUP'):
        self.meta = db.fetchreturn(qry['get'], id) or {}

    def merge_meta(self, data):
        if not self.meta:
            self.get_meta()
        merged_dict = self.meta.copy()
        for key, value in data.items():
            if key in merged_dict:
                merged_dict[key].update(value)
            else:
                merged_dict[key] = value
        self.meta = merged_dict
        return self

    def save_meta(self):
        if self.meta:
            db.execute(qry['upsert'], 'ROLLUP', json.jc(self.meta))
        return self

    def detect_dates_in_sheetnames(self, sheetnames):
        date_patterns = [
            (r'\b(\d{1,2})\.(\d{1,2})\.(\d{2})\b', "%m.%d.%y"),  # mm.dd.YY and m.dd.yy
            (r'\b(\d{1,2})\.(\d{1,2})\.(\d{4})\b', "%d.%m.%Y"),  # DD.MM.YYYY
            (r'\b(\d{1,2})-(\d{1,2})-(\d{2})\b', "%m-%d-%y"),  # MM-DD-YY
            (r'\b(\d{1,2})-(\d{1,2})-(\d{4})\b', "%m-%d-%Y"),  # MM-DD-YYYY
            (r'\b(\d{1,2})/(\d{1,2})/(\d{4})\b', "%m/%d/%Y"),  # MM/DD/YYYY
            (r'\b(\d{1,2}) (\d{1,2}) (\d{4})\b', "%m %d %Y"),  # MM DD YYYY
            (r'\b(\d{4})-(\d{1,2})-(\d{1,2})\b', "%Y-%m-%d"),  # YYYY-MM-DD
        ]

        detected_sheets = []

        for sheetname in sheetnames:
            for pattern, date_format in date_patterns:
                match = re.search(pattern, sheetname)
                if match:
                    try:
                        date_str = match.group(0)
                        date_obj = datetime.strptime(date_str, date_format)
                        reformatted_date_str = date_obj.strftime("%Y-%m-%d")
                        detected_sheets.append((sheetname, reformatted_date_str))
                    except ValueError:
                        continue
                    break

        return detected_sheets

    def authvstaff(self, data, date):
        key_value_pairs = []

        for row in data[:8]:
            start_index = 1 if row[0] is None else 0
            for i in range(start_index, len(row) - 1, 2):
                if row[i] is not None and row[i + 1] is not None:
                    key_value_pairs.append((row[i], row[i + 1]))

        parsed_dict = { re.sub(r'[^\w\s]', '', key).strip().upper(): value for key, value in key_value_pairs }
        for k in list(parsed_dict.keys()):
            if 'WEEK' in k:
                parsed_dict.pop(k, None)
        parsed_dict['WEEK_END'] = date
        try:
            key = f'{date}:{parsed_dict["LOCATION"]}'
        except:
            key = f'{date}:NA'
            for s,name in self.sites.items():
                if s in self.file_name:
                    key = f'{date}:{name}'
        return {key: parsed_dict}

    def budget(self, data, branch):
        branch = self.sites.get(branch.lower())
        budget_dict = {}
        headers = data[1]
        for row in data[2:]:
            for i in range(len(row)):
                if isinstance(row[i], datetime):
                    date = row[i].strftime("%Y-%m-%d")
                    if arrow.get(date) < arrow.get('2024-10-19'):
                        continue
                    budget_hours_index = i + 1
                    budget_hours = row[budget_hours_index] if budget_hours_index < len(row) else None
                    key = f'{date}:{branch}'
                    if key not in budget_dict:
                        budget_dict[key] = {}
                    period_label = headers[i + 1]
                    key = f'{date}:{branch}'
                    budget_dict[key] = {period_label.upper():budget_hours}
        return budget_dict

    def process_upload(self):
        xl = Excel('xlsx').open(self.file, data_only=True)
        self.file_name = xl.filename.lower()
        if 'auth' in self.file_name and 'staff' in self.file_name:
            detected_sheets = self.detect_dates_in_sheetnames(xl.list_sheets())
            for sheet, date in detected_sheets:
                if arrow.get(date) >= arrow.get('2024-10-19'):
                    meta = self.authvstaff(xl.ws(sheet), date)
                    self.merge_meta(meta)
        elif 'budget' in xl.filename.lower():
            for sheet in xl.list_sheets():
                if 'Budget' in sheet:
                    branch = sheet.split(' ')[0]
                    self.merge_meta(self.budget(xl.ws(sheet),branch.strip()))
        self.save_meta()
        return self.meta


if __name__ == "__main__":
    # x = ProcessFile('/Users/derekbartron/Documents/Viva/Import/2024 PDN Budget (2).xlsx').process_upload()
    # ProcessFile('/Users/derekbartron/Documents/Viva/Import/Austin Auth vs Staff.xlsx').process_upload()
    # ProcessFile('/Users/derekbartron/Documents/Viva/Import/Ft. Worth Auth vs Staff 2024.xlsx').process_upload()
    ProcessFile('/Users/derekbartron/Documents/Viva/Import/Richardson-Dallas Auth vs staff.xlsx').process_upload()
