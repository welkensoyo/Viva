import uuid
import logging
import arrow
import nfty.njson as json
from nfty.communications import upool
from api._config import smetrics as sm


logger = logging.getLogger("AppLogger")


class API:
    NOW = arrow.get().format("YYYY-MM-DD")
    CREATE_MERCHANT = {
        "_cc_email": "",
        "account_type": "New",
        "acct_open_date": NOW,
        "adc_30_day_deadline": NOW,
        "adc_90_day_deadline": NOW,
        "adc_date": NOW,
        "address": "",
        "address2": "",
        "address3": "",
        "bank_account_status": "Active",
        "brand": "",
        "brand2": "",
        "business_model": "",
        "category": "",
        "certification_route": "ISA",
        "chain_code": "",
        "channel": "",
        "channel_by_channel": True,
        "city": "",
        "company_name": "",
        "contact_name": "",
        "country": "",
        "dba_name": "",
        "domain_name": "",
        "ecommerce": 0,
        "email": "",
        "emv_percent_transactions": "",
        "ext_compliance": -1,
        "ext_compliance_exp": NOW,
        "ext_saq_exp_date": NOW,
        "ext_saq_rec_date": NOW,
        "ext_saq_type": "",
        "ext_saq_vendor": "",
        "ext_saq_version": "",
        "ext_scan_exp_date": NOW,
        "ext_scan_qty": 0,
        "ext_scan_rec_date": NOW,
        "ext_scan_vendor": "",
        "f2f": True,
        "fax": "",
        "headquarters": True,
        "isa_name": "",
        "last_pa_report_received": NOW,
        "level": 0,
        "mcc": 0,
        "mid": "",
        "misc1": "",
        "misc2": "",
        "misc3": 0,
        "misc4": 0,
        "misc5": "",
        "misc6": "",
        "moto": True,
        "next_contact_date": arrow.get().format("YYYY-MM-DD HH:mm:ssZZ"),
        "non_emv_number_transactions": 0,
        "online_presence": True,
        "p2pe_solution": True,
        "parent_account": "",
        "payment_processor": "",
        "pci_manager": "",
        "phone": "",
        "planned_compliance_date": NOW,
        "platform": "",
        "postal_code": "",
        "previous_pci_manager": "",
        "primary_mid": "",
        "prior_adc_level": "",
        "qsa_name": "",
        "reclassification_date": NOW,
        "repid": "",
        "repname": "",
        "risky": True,
        "roc_expiration": NOW,
        "roc_status": -1,
        "roc_vendor": "",
        "sad_removal_date": NOW,
        "sad_storage": "",
        "sad_storage_call_recordings_only": "Yes",
        "sales_office": "",
        "special_relationship": "",
        "state": "",
        "tip_participant": True,
        "trans_dollar_yearly_ae": 0,
        "trans_dollar_yearly_discover": 0,
        "trans_dollar_yearly_mc": 0,
        "trans_dollar_yearly_visa": 0,
        "trans_num_yearly_ae": 0,
        "trans_num_yearly_discover": 0,
        "trans_num_yearly_mc": 0,
        "trans_num_yearly_visa": 0,
    }

    def __init__(self, creds, clientid, reference=None, userhash=None):
        self.creds = creds
        self.clientid = clientid
        self.userhash = userhash
        self.reference = reference or str(uuid.uuid4())
        self.headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        self.preurl = sm.url

    def partners(self, last_modified_date=None):
        url = f"{self.preurl}/partners"
        meta = {}
        if last_modified_date:
            meta.update({"last_modified_date": last_modified_date})
        return self.transmit(url, meta)

    def partner(self, smetricid, create_new=None, merchants=False):
        meta = {}
        if merchants:
            url = f"{self.preurl}/partners/{smetricid}/merchants"
            if isinstance(merchants, (dict)):
                meta = dict(self.CREATE_MERCHANT)
                meta.update(merchants)
        else:
            url = f"{self.preurl}/partners/{smetricid}"
        if create_new:
            meta = {"preferred_name": create_new}
        return self.transmit(url, meta)

    def active_campaign_emails(self, smetricid, start=None):
        url = f"/partners/{smetricid}/active_campaign_emails"
        if start != None:
            meta = {"start": start}
            return self.transmit(url, meta, mode="PUT")
        return self.transmit(url)

    def merchant(self, smetricid, meta=None, delete=False):
        url = f"{self.preurl}/merchant/{smetricid}"
        if delete:
            return self.transmit(url, mode="DELETE")
        url = f"{self.preurl}/merchant/{smetricid}"
        if meta:
            return self.transmit(url, meta, mode="PUT")
        return self.transmit(url)

    def compliance(self, smetricid):
        url = f"{self.preurl}/merchant/{smetricid}/compliance"
        return self.transmit(url)

    def transmit(self, url, meta=None, mode="GET"):
        if meta:
            if mode == "GET":
                mode = "POST"
            meta = json.dumps(meta)
            r = upool.request(mode, url, body=meta, headers=self.headers, retries=3)
        else:
            r = upool.request(mode, url, headers=self.headers, retries=3)
        try:
            return r.data.decode()
        except ValueError as exc:
            logger.exception("Exception while making web request")
            return {"message": "Transaction Failed."}
