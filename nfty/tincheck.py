import os
import logging
from gevent import sleep
from zeep import Client, helpers
import nfty.constants as constants
from nfty.njson import jsonhtml
from nfty.communications import alert


logger = logging.getLogger("AppLogger")

user = "kb@tripleplaypay.com"
password = "Jber1234%"

cache = {}


class API:
    """
    This API interfaces with the site for TIN checks.
    """

    def __init__(self, enroll):
        self.e = enroll
        self.conn = Client(wsdl="https://www.tincheck.com/pvsws/pvsservice.asmx?wsdl")
        self.factory = self.conn.type_factory("ns0")
        self.user = self.factory.UserClass(UserLogin=user, UserPassword=password)

    @classmethod
    def enrollment(cls, id):
        from nfty.onboarding import Enroll

        return cls(Enroll(id))

    @classmethod
    def client(cls, id):
        from nfty.clients import Client

        return cls(Client(id).enroll())

    def services(self):
        return dir(self.conn.service)

    def status(self):
        return self.conn.service.ServiceStatus(self.user)

    def validateTinName(self, meta):
        TinNameClass = self.factory.TinNameClass(
            TIN=meta.get("fed_tx_id").replace("-", "").strip(),
            LName=meta.get("legal_name").strip(),
        )
        kyc = self.conn.service.ValidateTinName(TinName=TinNameClass, CurUser=self.user)
        kyc = helpers.serialize_object(kyc, target_cls=dict)
        return kyc

    def validateTinNameAddressListMatch(self, meta):
        TinNameClass = self.factory.TinNameClass(
            TIN=meta.get("fed_tx_id").replace("-", "").strip(),
            LName=meta.get("legal_name").strip(),
        )
        USPSAddress = self.factory.USPSAddressClass(
            Address1=meta.get("business_address_1"),
            Address2=meta.get("business_address_2"),
            City=meta.get("business_city"),
            State=meta.get("business_state_province"),
            Zip5=meta.get("business_postal_code"),
        )
        kyc = self.conn.service.ValidateTinNameAddressListMatch(
            TinName=TinNameClass, CurUser=self.user, USPSAddress=USPSAddress
        )
        kyc = helpers.serialize_object(kyc, target_cls=dict)
        if kyc["TINNAME_RESULT"]["TINNAME_CODE"] not in (
            "17",
            17,
        ):  # TODO: Discover significance behind 17
            return kyc
        sleep(30)
        return self.validateTinNameAddressListMatch(meta)

    def run(self):
        try:
            kyc = self.validateTinNameAddressListMatch(self.e.meta)
        except AttributeError:
            logger.exception("AttributeError thrown while running TINCHECK")
            kyc = self.validateTinNameAddressListMatch(self.e)
        if "TINNAME_RESULT" in kyc:
            kyc.pop("STATUS", "")
            result = {"tincheck": kyc}
            alert(
                f"TIN CHECK: NO RESULT RETURNED FOR {self.e.meta.get('dba_name')}",
                f"TIN CHECK RESULTS FOR {self.e.meta['dba_name']}:<br>{jsonhtml(result)}",
                constants.EMAIL_TPP_TEAM,
            )
            return result
        else:
            alert(
                f"TIN CHECK: NO RESULT RETURNED FOR {self.e.meta.get('dba_name')}",
                f"NO TIN CHECK RESULTS FOR {self.e.meta.get('dba_name')}",
                constants.EMAIL_TPP_TEAM,
            )
        return {}

    def checkonly(self):
        return self.validateTinName(self.e.meta)


def manual(eid, client=True):
    """
    This is intended on being called manually from the CLI.
    """
    logger.warning("Manual Intervention on enrolling")

    os.chdir("../")
    if not client:
        e = API.enrollment(eid)
        kyc = e.run()
        alert(
            f"TIN CHECK : {e.e.meta['dba_name']}",
            f"TIN CHECK RESULTS FOR {e.e.meta['dba_name']}:<br>{jsonhtml(kyc)}",
            constants.EMAIL_TPP_TEAM,
        )
    else:
        e = API.client(eid)
        kyc = e.run()
        alert(
            f"TIN CHECK : {e.e['dba_name']}",
            f"TIN CHECK RESULTS FOR {e.e['dba_name']}:<br>{jsonhtml(kyc)}",
            constants.EMAIL_TPP_TEAM,
        )
    return kyc


# if __name__ == "__main__":
#     # e = API.client('2d464bd1-a433-4993-863e-9776af3bb229')
#     manual("9246d6f5-2e28-4534-9833-6617862fb3dd")

# manual('db9a1c24-833f-46c8-abc7-abc32133aeee')
# tin = API.enrollment('c0c37477-b223-4194-aac2-5401460a6067').run()
# pprint({'REQUESTID': 159859945, 'REQUEST_STATUS': 1, 'REQUEST_DETAILS': 'Request Completed', 'TINNAME_RESULT': {'TINNAME_CODE': 0, 'TINNAME_DETAILS': 'No IRS Match found. TIN and Name combination does not match IRS records', 'DMF_CODE': 0, 'DMF_DETAILS': 'No Death Master File Match Found.', 'DMF_DATA': '\n          ', 'EIN_CODE': 1, 'EIN_DETAILS': 'EIN Possible Match found.', 'EIN_DATA': 'JOSE R HIDALGO', 'GIIN_CODE': -1, 'GIIN_DETAILS': 'No GIIN provided. GIIN lookup skipped.', 'GIIN_DATA': None, 'STATUS': None}, 'LISTMATCH_RESULT': {'LISTSMATCH_CODE': 0, 'LISTSMATCH_DETAILS': 'No Lists Matches Found', 'OFAC_RESULT': {'OFAC_CODE': 0, 'OFAC_DETAILS': 'Department of Treasury, Office of Foreign Assets Control (OFAC SDN/PLC): Found 0 possible matches', 'OFAC_COUNT': 0, 'OFAC_DATA': '\n            '}, 'NV_RESULT': {'NV_CODE': 0, 'NV_DETAILS': 'Nevada Gaming Control Board (GBC) Excluded Persons List: Found 0 possible matches', 'NV_COUNT': 0, 'NV_DATA': '\n            '}, 'MS_RESULT': {'MS_CODE': 0, 'MS_DETAILS': 'Mississippi Gaming Commission Exclusion List: Found 0 possible matches', 'MS_COUNT': 0, 'MS_DATA': '\n            '}, 'IL_RESULT': {'IL_CODE': 0, 'IL_DETAILS': 'Illinois Gaming Board Exclusion List: Found 0 possible matches', 'IL_COUNT': 0, 'IL_DATA': '\n            '}, 'MO_RESULT': {'MO_CODE': 0, 'MO_DETAILS': 'Missouri Gaming Commission Exclusion List: Found 0 possible matches', 'MO_COUNT': 0, 'MO_DATA': '\n            '}, 'NJ_RESULT': {'NJ_CODE': 0, 'NJ_DETAILS': 'New Jersey Casino Control Commission Exclusion List: Found 0 possible matches', 'NJ_COUNT': 0, 'NJ_DATA': '\n            '}, 'EPLS_RESULT': None, 'DPL_Result': None, 'PEP_Result': None, 'HHS_RESULT': None, 'STATUS': None, 'Results': {'Result': [{'Type': 'OFAC', 'Code': 0, 'Details': 'Department of Treasury, Office of Foreign Assets Control (OFAC SDN/PLC): Found 0 possible matches', 'Count': 0, 'Data': '\n              '}, {'Type': 'HHS', 'Code': 0, 'Details': 'Health &amp; Human Services (HHS), Office of the Inspector General (OIG), Excluded Individuals and Entities (LEIE): Found 0 possible matches', 'Count': 0, 'Data': '\n              '}, {'Type': 'PEP', 'Code': 0, 'Details': 'Politically Exposed Persons. (foreign diplomats): Found 0 possible matches', 'Count': 0, 'Data': '\n              '}, {'Type': 'DPL', 'Code': 0, 'Details': 'Department of Commerce (DOC), Denied Persons List (DPL): Found 0 possible matches', 'Count': 0, 'Data': '\n              '}, {'Type': 'EPLS', 'Code': 0, 'Details': 'General Services Administration (GSA), Excluded Parties List System (EPLS): Found 0 possible matches', 'Count': 0, 'Data': '\n              '}, {'Type': 'DFTO', 'Code': 0, 'Details': 'Designated Foreign Terrorist Organizations: Found 0 possible matches', 'Count': 0, 'Data': '\n              '}, {'Type': 'FSE', 'Code': 0, 'Details': 'Foreign Sanctions Evaders (FSE) List: Found 0 possible matches', 'Count': 0, 'Data': '\n              '}, {'Type': 'GIIN', 'Code': 0, 'Details': 'IRS FATCA GIIN List: Found 0 possible matches', 'Count': 0, 'Data': '\n              '}, {'Type': 'EUS', 'Code': 0, 'Details': 'EU Sanctions List: Found 0 possible matches', 'Count': 0, 'Data': '\n              '}, {'Type': 'DTC', 'Code': 0, 'Details': 'Arms Export Control Act List: Found 0 possible matches', 'Count': 0, 'Data': '\n              '}, {'Type': 'FFI', 'Code': 0, 'Details': 'Foreign Financial Institutions Subj. 561/CAPTA List: Found 0 possible matches', 'Count': 0, 'Data': '\n              '}, {'Type': 'IEO', 'Code': 0, 'Details': 'IRS Exempt Organizations List: Found 0 possible matches', 'Count': 0, 'Data': '\n              '}, {'Type': 'ISA', 'Code': 0, 'Details': 'Iranian Sanctions Act List: Found 0 possible matches', 'Count': 0, 'Data': '\n              '}, {'Type': 'SSI', 'Code': 0, 'Details': 'Sectoral Sanctions Identifications List: Found 0 possible matches', 'Count': 0, 'Data': '\n              '}, {'Type': 'UVL', 'Code': 0, 'Details': 'Unverified List: Found 0 possible matches', 'Count': 0, 'Data': '\n              '}, {'Type': 'EAR', 'Code': 0, 'Details': 'Client List: Found 0 possible matches', 'Count': 0, 'Data': '\n              '}, {'Type': 'PLC', 'Code': 0, 'Details': 'Palestinian Legislative Council List: Found 0 possible matches', 'Count': 0, 'Data': '\n              '}, {'Type': 'UKE', 'Code': 0, 'Details': 'Ukraine-Russia Related Sanctions List: Found 0 possible matches', 'Count': 0, 'Data': '\n              '}, {'Type': 'ATF', 'Code': 0, 'Details': 'Anti-Terrorism Financing: Found 0 possible matches', 'Count': 0, 'Data': '\n              '}, {'Type': 'NPS', 'Code': 0, 'Details': 'Iran-Syria Nonproliferation Act: Found 0 possible matches', 'Count': 0, 'Data': '\n              '}, {'Type': 'ATFE', 'Code': 0, 'Details': 'Anti-Terrorism Financing Entities: Found 0 possible matches', 'Count': 0, 'Data': '\n              '}, {'Type': 'UNCON', 'Code': 0, 'Details': 'United Nations Consolidated: Found 0 possible matches', 'Count': 0, 'Data': '\n              '}, {'Type': 'TCON', 'Code': 0, 'Details': 'Consolidated Sanctions List: Found 0 possible matches', 'Count': 0, 'Data': '\n              '}, {'Type': 'CFTCSIRT', 'Code': 0, 'Details': 'Administrative Sanctions: Found 0 possible matches', 'Count': 0, 'Data': '\n              '}, {'Type': 'CFTCSIRTR', 'Code': 0, 'Details': 'Reparations Sanctions: Found 0 possible matches', 'Count': 0, 'Data': '\n              '}, {'Type': 'INT', 'Code': 0, 'Details': 'Wanted: Found 0 possible matches', 'Count': 0, 'Data': '\n              '}, {'Type': 'JLAWSOR', 'Code': 0, 'Details': 'Consolidated Regulations: Found 0 possible matches', 'Count': 0, 'Data': '\n              '}, {'Type': 'SEMA', 'Code': 0, 'Details': 'Special Economic Measures Act: Found 0 possible matches', 'Count': 0, 'Data': '\n              '}, {'Type': 'FACFOA', 'Code': 0, 'Details': 'Freezing Assets of Corrupt Foreign Officials Act: Found 0 possible matches', 'Count': 0, 'Data': '\n              '}, {'Type': 'FBIMWT', 'Code': 0, 'Details': 'Most Wanted Terrorists: Found 0 possible matches', 'Count': 0, 'Data': '\n              '}, {'Type': 'FBIWCC', 'Code': 0, 'Details': 'White Collar Crimes: Found 0 possible matches', 'Count': 0, 'Data': '\n              '}, {'Type': 'FBIAVC', 'Code': 0, 'Details': 'Additional Violent Crimes: Found 0 possible matches', 'Count': 0, 'Data': '\n              '}, {'Type': 'FBICA', 'Code': 0, 'Details': 'Crime Alerts: Found 0 possible matches', 'Count': 0, 'Data': '\n              '}, {'Type': 'FBICAC', 'Code': 0, 'Details': 'Crimes Against Children: Found 0 possible matches', 'Count': 0, 'Data': '\n              '}, {'Type': 'FBICC', 'Code': 0, 'Details': 'Cyber Crimes: Found 0 possible matches', 'Count': 0, 'Data': '\n              '}, {'Type': 'FBICEI', 'Code': 0, 'Details': 'Criminal Enterprise Investigations: Found 0 possible matches', 'Count': 0, 'Data': '\n              '}, {'Type': 'FBIDT', 'Code': 0, 'Details': 'Domestic Terrorism: Found 0 possible matches', 'Count': 0, 'Data': '\n              '}, {'Type': 'FBISI', 'Code': 0, 'Details': 'Seeking Information: Found 0 possible matches', 'Count': 0, 'Data': '\n              '}, {'Type': 'FBISTI', 'Code': 0, 'Details': 'Seeking Terror Information: Found 0 possible matches', 'Count': 0, 'Data': '\n              '}, {'Type': 'FBITMW', 'Code': 0, 'Details': 'Top Ten Fugitives: Found 0 possible matches', 'Count': 0, 'Data': '\n              '}, {'Type': 'FBIVCM', 'Code': 0, 'Details': 'Violent Crimes - Murders: Found 0 possible matches', 'Count': 0, 'Data': '\n              '}, {'Type': 'NV', 'Code': 0, 'Details': 'Nevada Gaming Control Board (GBC) Excluded Persons List: Found 0 possible matches', 'Count': 0, 'Data': '\n              '}, {'Type': 'MS', 'Code': 0, 'Details': 'Mississippi Gaming Commission Exclusion List: Found 0 possible matches', 'Count': 0, 'Data': '\n              '}, {'Type': 'IL', 'Code': 0, 'Details': 'Illinois Gaming Board Exclusion List: Found 0 possible matches', 'Count': 0, 'Data': '\n              '}, {'Type': 'MO', 'Code': 0, 'Details': 'Missouri Gaming Commission Exclusion List: Found 0 possible matches', 'Count': 0, 'Data': '\n              '}, {'Type': 'NJ', 'Code': 0, 'Details': 'New Jersey Casino Control Commission Exclusion List: Found 0 possible matches', 'Count': 0, 'Data': '\n              '}]}}, 'ADDRESS_RESULT': {'ADDRESS_CODE': 2, 'ADDRESS_DETAILS': 'USPS Match found. Address verified as valid, checked for proper Postal Service format and standardized if necessary  5 FORDHAM HILL OVAL APT 11F BRONX, NY 10468-4762 16'}})
