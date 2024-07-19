import uuid
import traceback
import urllib3
import xmltodict
import logging
import nfty.constants as constants
import nfty.clients as clients
import nfty.njson as json
from nfty.communications import upool, alert
from api._config import kyc, env
from nfty.docs import Document
from nfty.tincheck import API as Tincheck


logger = logging.getLogger("AppLogger")


class API:
    """
    This class encapsulates all logic for Know Your Customer (KYC) checks.

        -- https://wsonline.seisint.com/DEMO/WsAccurint
        -- https://wsonline.seisint.com/WsAccurint

    Depends on the `Enroll` class directly.

        -- dba_name
        -- email
        -- meta
    """

    def __init__(self, enroll, mode=constants.MODE_ENROLL):
        self.enroll = enroll
        self.kyc = kyc
        self.mode = mode
        self.qa = False if env == constants.PRODUCTION else True
        self.message = ""
        self.id = self.enroll.id
        self.dba_name = self.enroll.meta.get("dba_name")
        self.email = self.enroll.meta.get("email")
        self.meta = self.enroll.meta
        self.result = {}
        self.pdf = ""
        self.rt = ["KYC", "EMPTY", constants.KYC_HAS_NOT_RUN_YET, ""]
        if constants.KYC in self.meta:
            self.result.update(self.meta["kyc"])
            self.rt = self.meta["kyc"].get("risk_table") or []

    @classmethod
    def client(cls, id):
        client = clients.Client(id)
        kyc = cls(client, mode="client")
        cls.meta = client.enroll()
        cls.id = client.id
        cls.dba_name = client.name
        cls.email = client.email
        return kyc

    def run(self, rerun=False):
        """
        This runs the KYC Check.

        It will check for required fields and optionally run a tincheck if an additional field is passed
        """
        if self.qa:
            return self
        mandatory = (
            "dba_name",
            "legal_name",
            "principal_first_name",
            "principal_last_name",
            "fed_tx_id",
            "email",
            "principal_ssn",
            "business_address_1",
        )
        for m in mandatory:
            if not self.meta.get(m):
                return self  # TODO: Have this return an error
        if self.meta.get("principal_ssn"):
            self.person(rerun=rerun)
            self.business(rerun=rerun)
        if self.meta.get("fed_tx_id"):
            self.tincheck(rerun=rerun)
        self.risk_parser()
        return self

    def passed_kyc(self):
        if self.rt:
            return False
        return True

    def tincheck(self, rerun=False):
        if self.qa:
            return self
        if not rerun and "tincheck" in self.result:
            if self.result["tincheck"]:
                return self
        tincheck_results = Tincheck(self.enroll).run()
        self.result.update(tincheck_results)
        try:
            try:
                self.result["tincheck"]["TINNAME_RESULT"][
                    "DMF_DATA"
                ] = "SEE PORTAL FOR INFO"
            except:
                logger.exception("Exception thrown with TINNAME_RESULT Results")
            file = Document.dict2file(self.result["tincheck"])
            filename = f"{self.meta['dba_name']}-kyc-tincheck-{json.ctnow().format('YYYY-MM-DD_HH:mm:ss')}.txt"
            Document.save_kyc(file, filename, self.id)
        except Exception as e:
            logger.exception("Exception thrown with TINCheck Results")
            traceback.print_exc()

        alert(
            f"TIN CHECK : {self.meta['dba_name']}",
            f"TIN CHECK RESULTS FOR {self.meta['dba_name']}:<br>{json.jsonhtml(tincheck_results)}",
            constants.EMAIL_TEAM,
        )

        return self

    def get(self):
        if "BIID20ResponseEx" in self.result:
            return self.result["BIID20ResponseEx"]["response"]["Result"]
        return {}

    def person(self, rerun=False):
        if self.qa:
            return self
        if not rerun and "RollupPersonSearchResponseEx" in self.result:
            return self

        def full_ssn(k):
            try:
                self.pdf = self.result["RollupPersonSearchResponseEx"]["response"].pop(
                    "Pdf", ""
                )
                self.save_pdf(
                    f"{self.meta['dba_name']}-kyc-{self.meta[k+'principal_last_name']}-{json.ctnow().format('YYYY-MM-DD_HH:mm:ss')}.pdf"
                )
                for r in self.result["RollupPersonSearchResponseEx"]["response"][
                    "Records"
                ]["Record"]:
                    for s in r["SSNs"]["EnhancedSSNInfo"]:
                        if s.get("SSN"):
                            return s["SSN"].replace(
                                "xxxx", self.meta[k + "principal_ssn"][-4:]
                            )
            except Exception as e:
                logger.exception("Exception while attempting to access full_ssn value")
                return None

        self.headers = urllib3.make_headers(
            basic_auth=f"{self.kyc.uid}:{self.kyc.upassword}"
        )
        self.headers.update(
            {
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )
        persons = [
            self.meta.get(person)
            for person in (
                "principal_first_name",
                "2principal_first_name",
                "3principal_first_name",
                "4principal_first_name",
            )
            if self.meta.get(person)
        ]
        keys = (
            "principal_date_of_birth",
            "principal_first_name",
            "principal_last_name",
            "principal_address_1",
            "principal_address_2",
            "principal_city",
            "principal_state_province",
            "principal_postal_code",
            "principal_ssn",
            "principal_phone_number",
            "ssn",
        )
        for i, person in enumerate(persons):
            _ = str(i + 1) if i else ""
            try:
                dob = json.clean_date(self.meta.get(_ + keys[0])).split("-")
            except:
                continue
            meta = {
                "RollupPersonSearchRequest": {
                    "User": {
                        "ReferenceCode": self.meta.get('reference', ''),
                        "BillingCode": self.id,
                        "QueryId": str(uuid.uuid4()),
                        "GLBPurpose": "3",
                        "DLPurpose": "3",
                        "Enduser": {
                            "CompanyName": "Payment Made Simple Inc",
                            "StreetAdress1": "1990 Rock Springs Rd",
                            "City": "Columbia",
                            "State": "TN",
                            "Zip5": "38401",
                        },
                        "MaxWaitSeconds": 30,
                        "OutputType": "B",
                    },
                    "Options": {
                        "StrictMatch": False,
                        "SSNTypos": False,
                        "ReturnCount": 10,
                        "StartingRecord": 1,
                        "ScoreThreshold": 0,
                        "IncludeFullHistory": False,
                        "IncludeRelativeNames": False,
                        "UsePhonetics": False,
                        "IncludeRecordsWithoutADL": False,
                        "UseNicknames": False,
                        "BlanksFilledIn": False,
                        "IncludeHousehold": False,
                        "ReducedData": False,
                        "IncludeDriverLicense": False,
                        "IncludeNonDMVSources": False,
                        "CurrentResidentsOnly": False,
                        "IncludeFraudDefenseNetwork": False,
                        "IncludeBusinessCredit": False,
                        "AddressLimit": 0,
                    },
                    "SearchBy": {
                        "Name": {
                            "Full": f"{self.meta.get(_+keys[1])} {self.meta.get(_+keys[2])}"
                        },
                        "Address": {
                            "StreetAddress1": self.meta.get(_ + keys[3]) or "",
                            "StreetAddress2": self.meta.get(_ + keys[4]) or "",
                            "City": self.meta.get(_ + keys[5]) or "",
                            "State": self.meta.get(_ + keys[6]) or "",
                            "Zip5": self.meta.get(_ + keys[7]) or "",
                        },
                        "SSNLast4": str(self.meta.get(_ + keys[8])).strip()[-4:],
                        "Phone10": self.meta.get(_ + keys[9]),
                        "DOB": {"Year": dob[0], "Month": dob[1], "Day": dob[2]},
                    },
                }
            }
            self.transmit(self.kyc.peoplesearch, meta)  # Use KYC
            self.meta[_ + keys[10]] = full_ssn(_)
        return self

    def dob_format(self, date):
        if not date:
            return "--"
        if "/" in date:
            date = date.split("/")
            date = f"{date[2]}-{date[0]}-{date[1]}"
        if "\\" in date:
            date = date.split("\\")
            date = f"{date[2]}-{date[0]}-{date[1]}"
        return date

    def business(self, rerun=False):
        if self.qa:
            return self
        if not rerun and "BIID20ResponseEx" in self.result:
            return self
        self.headers = urllib3.make_headers(
            basic_auth=f"{self.kyc.id}:{self.kyc.password}"
        )
        self.headers.update(
            {
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )
        meta = {
            "BusinessInstantID20Request": {
                "User": {
                    "ReferenceCode": "TriplePlayPay",
                    "BillingCode": self.id,
                    "QueryId": str(uuid.uuid4()),
                    "GLBPurpose": "6",
                    "DLPurpose": "6",
                    "Enduser": {
                        "CompanyName": "Payment Made Simple Inc",
                        "StreetAdress1": "1990 Rock Springs Rd",
                        "City": "Columbia",
                        "State": "TN",
                        "Zip5": "38401",
                    },
                    "MaxWaitSeconds": 30,
                    "AccountNumber": "1838062",
                    "OutputType": "B",
                },
                "Options": {
                    "ExactAddrMatch": False,
                    "ExactDOBMatch": False,
                    "ExactDriverLicenseMatch": False,
                    "ExactFirstNameMatch": False,
                    "ExactFirstNameMatchAllowNicknames": False,
                    "ExactLastNameMatch": False,
                    "ExactPhoneMatch": False,
                    "ExactSSNMatch": True,
                    "ExcludeWatchLists": False,
                    "IncludeAdditionalWatchlists": False,
                    "IncludeCLOverride": False,
                    "IncludeDLVerification": False,
                    "IncludeDOBInCVI": False,
                    "IncludeDPBC": False,
                    "IncludeDriverLicenseInCVI": False,
                    "IncludeMIOverride": False,
                    "IncludeMSOverride": False,
                    "IncludeOFAC": True,
                    "LastSeenThreshold": "",
                    "NameInputOrder": "",
                    "PoBoxCompliance": False,
                    "UseDOBFilter": False,
                },
                "SearchBy": {
                    "Company": {
                        "CompanyName": self.meta["legal_name"],
                        "Address": {
                            "StreetAddress1": self.meta.get("business_address_1"),
                            "StreetAddress2": self.meta.get("business_address_2"),
                            "City": self.meta.get("business_city"),
                            "State": self.meta.get("business_state_province"),
                            "Zip5": self.meta.get("business_postal_code"),
                        },
                        "Phone": self.meta.get("business_phone_number").replace(
                            "-", ""
                        ),
                        "FEIN": self.meta.get("fed_tx_id", "").replace("-", ""),
                    },
                    "AuthorizedRep1": {
                        "Sequence": None,
                        "Name": {
                            "Full": f"{self.meta.get('principal_first_name')} {self.meta.get('principal_last_name')}",
                        },
                        "Address": {
                            "StreetAddress1": self.meta.get("principal_address_line_1"),
                            "StreetAddress2": self.meta.get("principal_address_line_2"),
                            "City": self.meta.get("principal_city"),
                            "State": self.meta.get("principal_state_province"),
                            "Zip5": self.meta.get("principal_postal_code"),
                        },
                        "DOB": {
                            "Year": self.dob_format(
                                self.meta.get("principal_date_of_birth", "")
                            ).split("-")[0],
                            "Month": self.dob_format(
                                self.meta.get("principal_date_of_birth", "")
                            ).split("-")[1],
                            "Day": self.dob_format(
                                self.meta.get("principal_date_of_birth", "")
                            ).split("-")[2],
                        },
                        "Email": self.meta.get("email"),
                        "SSN": self.meta.get("ssn") or "",
                    },
                },
            }
        }
        x = {}
        if self.meta.get("2principal_first_name"):
            x.update(
                {
                    "AuthorizedRep2": {
                        "Sequence": None,
                        "Name": {
                            "Full": f"{self.meta.get('2principal_first_name')} {self.meta.get('2principal_last_name')}",
                        },
                        "Address": {
                            "StreetAddress1": self.meta.get(
                                "2principal_address_line_1"
                            ),
                            "StreetAddress2": self.meta.get(
                                "2principal_address_line_2"
                            ),
                            "City": self.meta.get("2principal_city"),
                            "State": self.meta.get("2principal_state_province"),
                            "Zip5": self.meta.get("2principal_postal_code"),
                        },
                        "DOB": {
                            "Year": self.dob_format(
                                self.meta.get("2principal_date_of_birth", "")
                            ).split("-")[0],
                            "Month": self.dob_format(
                                self.meta.get("2principal_date_of_birth", "")
                            ).split("-")[1],
                            "Day": self.dob_format(
                                self.meta.get("2principal_date_of_birth", "")
                            ).split("-")[2],
                        },
                        "SSN": self.meta.get("2ssn") or "",
                    }
                }
            )
        if self.meta.get("3principal_first_name"):
            x.update(
                {
                    "AuthorizedRep3": {
                        "Sequence": None,
                        "Name": {
                            "Full": f"{self.meta.get('3principal_first_name')} {self.meta.get('3principal_last_name')}",
                        },
                        "Address": {
                            "StreetAddress1": self.meta.get(
                                "3principal_address_line_1"
                            ),
                            "StreetAddress2": self.meta.get(
                                "3principal_address_line_2"
                            ),
                            "City": self.meta.get("3principal_city"),
                            "State": self.meta.get("3principal_state_province"),
                            "Zip5": self.meta.get("3principal_postal_code"),
                        },
                        "DOB": {
                            "Year": self.dob_format(
                                self.meta.get("3principal_date_of_birth", "")
                            ).split("-")[0],
                            "Month": self.dob_format(
                                self.meta.get("3principal_date_of_birth", "")
                            ).split("-")[1],
                            "Day": self.dob_format(
                                self.meta.get("3principal_date_of_birth", "")
                            ).split("-")[2],
                        },
                        "SSN": self.meta.get("3ssn") or "",
                    }
                }
            )
        if self.meta.get("4principal_first_name"):
            x.update(
                {
                    "AuthorizedRep4": {
                        "Sequence": None,
                        "Name": {
                            "Full": f"{self.meta.get('4principal_first_name')} {self.meta.get('4principal_last_name')}",
                        },
                        "Address": {
                            "StreetAddress1": self.meta.get(
                                "4principal_address_line_1"
                            ),
                            "StreetAddress2": self.meta.get(
                                "4principal_address_line_2"
                            ),
                            "City": self.meta.get("4principal_city"),
                            "State": self.meta.get("4principal_state_province"),
                            "Zip5": self.meta.get("4principal_postal_code"),
                        },
                        "DOB": {
                            "Year": self.dob_format(
                                self.meta.get("4principal_date_of_birth", "")
                            ).split("-")[0],
                            "Month": self.dob_format(
                                self.meta.get("4principal_date_of_birth", "")
                            ).split("-")[1],
                            "Day": self.dob_format(
                                self.meta.get("4principal_date_of_birth", "")
                            ).split("-")[2],
                        },
                        "SSN": self.meta.get("4ssn") or "",
                    }
                }
            )
        if x:
            meta["BusinessInstantID20Request"]["SearchBy"].update(x)
        x = self.transmit(self.kyc.bzurl, meta)
        if "BIID20ResponseEx" in self.result:
            self.pdf = self.result["BIID20ResponseEx"]["response"].pop("Pdf", "")
            if self.pdf:
                self.save_pdf(
                    f"{self.meta['dba_name']}-kyc-business-{json.ctnow().format('YYYY-MM-DD_HH:mm:ss')}.pdf"
                )
        return x

    def transmit(self, url, meta, mode="POST"):
        meta = json.dumps(meta or {}).encode("utf-8")
        r = upool.request(mode, url, body=meta, headers=self.headers, retries=3)
        try:
            results = json.dc(r.data.decode())
            self.result.update(results)
            alert(
                f"KYC CHECK : {self.meta['dba_name']}",
                f"KYC CHECK RESULTS FOR {self.meta['dba_name']}:<br>{json.jsonhtml(results)}",
                constants.EMAIL_TEAM,
            )
        except:
            results = json.dc(r.data)
            self.result.update(results)
            alert(
                f"KYC CHECK : {self.meta['dba_name']}",
                f"KYC CHECK RESULTS FOR {self.meta['dba_name']}:<br>{json.jsonhtml(results)}",
                constants.EMAIL_TEAM,
            )
        return self

    def save_enroll(self):
        self.enroll.save({"kyc": self.result, "ssn": self.meta.get("ssn")})
        return self

    def save(self):
        self.meta["kyc"].save(self.result)
        c = clients.Client(self.id)
        c.enroll(self.meta)
        return self

    def save_pdf(self, filename):
        if self.pdf:
            return Document.save_kyc(self.pdf, filename, self.id)

    def xml2json(self, meta):
        return json.loads(json.dumps(xmltodict.parse(meta)))

    def risk_update(self, meta):
        if meta:
            self.result["risk_table"] = json.lc(meta)
            return self
        return self.risk_parser()

    def risk_parser(self):  # TODO: need to simplify
        def check_int(v):
            try:
                return int(v)
            except:
                return 0

        failure_code = {
            2,
            3,
            6,
            12,
            14,
            15,
            19,
            20,
            21,
            22,
            23,
            24,
            26,
            27,
            28,
            29,
            32,
            34,
            37,
            38,
            39,
            42,
            43,
            48,
            50,
            51,
            52,
            54,
            58,
            59,
            60,
            85,
            89,
            90,
            91,
            92,
            93,
            94,
            95,
            96,
            103,
            104,
            106,
            107,
            108,
            109,
            115,
            120,
            125,
            130,
            135,
            140,
            145,
            150,
            155,
            160,
        }
        if "tincheck" in self.result:
            self.rt = []
            if str(self.result["tincheck"]["TINNAME_RESULT"]["TINNAME_CODE"]) == "0":
                self.rt.append(["Tincheck", "FAILED", "Company TIN not found.", ""])
            if (
                str(self.result["tincheck"]["LISTMATCH_RESULT"]["LISTSMATCH_CODE"])
                != "0"
            ):
                self.rt.append(["Tincheck", "NAUGHTY", "Company on a Naughty List", ""])
        if "RollupPersonSearchResponseEx" in self.result:
            if (
                str(
                    self.result["RollupPersonSearchResponseEx"]["response"][
                        "SubjectTotalCount"
                    ]
                )
                == "0"
            ):
                self.rt.append(["Person", "SSN", "No SSN matched", ""])
        if "BIID20ResponseEx" in self.result:
            for k, v in self.result["BIID20ResponseEx"]["response"]["Result"].items():
                if k == "CompanyResults":
                    for a, _ in (v.get("AddressRisk") or {}).items():
                        if check_int(a) in failure_code:
                            self.rt.append(["AddressRisk", a, _, ""])
                    try:
                        for r in v["RiskIndicators"]["RiskIndicator"]:
                            if check_int(r["RiskCode"]) in failure_code:
                                self.rt.append(
                                    [
                                        "RiskIndicator",
                                        r["RiskCode"],
                                        r["Description"],
                                        "",
                                    ]
                                )
                    except KeyError:
                        logger.exception("Error thrown reading BIID20ResponseEx")
                        continue
                try:
                    if k == "AuthorizedRepresentativeResults":
                        for auth in v["AuthorizedRepresentativeResult"]:
                            for a, _ in (auth.get("AddressRisk") or {}).items():
                                if check_int(a) in failure_code:
                                    self.rt.append(["PrimaryAddressRisk", a, _, ""])

                                for r in auth["RiskIndicators"]["RiskIndicator"]:
                                    if check_int(r["RiskCode"]) in failure_code:
                                        self.rt.append(
                                            [
                                                "RiskIndicator",
                                                r["RiskCode"],
                                                r["Description"],
                                                "",
                                            ]
                                        )
                                for r in auth["PotentialFollowupActions"][
                                    "PotentialFollowupAction"
                                ]:
                                    if check_int(r["RiskCode"]) in failure_code:
                                        self.rt.append(
                                            [
                                                "FollowUp",
                                                r["RiskCode"],
                                                r["Description"],
                                                "",
                                            ]
                                        )
                except KeyError:
                    logger.exception("Exception thrown reading RiskCode")
                    continue
            self.result["risk_table"] = self.rt
        return self

    def output(self, i=4):
        return json.dumps(self.result, indent=i)


def redo(option):
    """
    This retries EVERY KYC entry.
    """
    import nfty.db as db

    logger.warning("redo ALL KYC checks has been called")
    PSQL = "SELECT id FROM entity.enroll"
    all_entity_ids = db.fetchall(PSQL)
    for entity_ids in all_entity_ids:
        try:
            if option == "person":
                API.enrollment(entity_ids[0]).person(rerun=False).save_enroll()
            if option == "business":
                API.enrollment(entity_ids[0]).business(rerun=False).save_enroll()
        except Exception as exc:
            logger.exception(exc)
