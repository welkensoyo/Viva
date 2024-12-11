from gevent import sleep
import arrow
from nfty.communications import upool
import nfty.njson as json
import urllib3
from urllib.parse import urlencode
from api._config import paycor as config


class API:
    def __init__(self):
        self.error = None
        self.pre_url = 'https://apis.paycor.com'
        self.header = {
            'Content-Type': "application/json",
            'Ocp-Apim-Subscription-Key': '94624d3fedee456b86d51fdc7f89be9a',
            'Authorization' : 'Bearer lBpKk0yyzZmprMCmqARNxNqBlh0x6ugswhwdQ3AFFQ'
        }
        self.token = ''
        self.session()

    def session(self):
        if not self.token:
            url = 'https://apis.paycor.com/sts/v1/common/token'
            x = self.transmit(url, meta={}, mode='POST')
            print(x)
            if 'response' in x:
                self.token = 'Bearer '+x['response']['token']
                self.header['Authorization'] = self.token
        return self

    def _build_url(self, route):
        return f"{self.pre_url}{route}"

    def get_hours(self, employee_id, planner_id=None, pay_number=0, continuation_token=None):
        params = {"employeeId": employee_id}
        if planner_id:
            params["plannerId"] = planner_id
        if pay_number > 0:
            params["payNumber"] = pay_number
        if continuation_token:
            params["continuationToken"] = continuation_token

        url = self._build_url("employees/hours") + "?" + urlencode(params)
        return self.transmit(url)

    def update_legal_entity_work_location(self, id, data):
        url = self._build_url(f"legal-entities/{id}/work-locations")
        return self.transmit(url, meta=data, mode='POST')

    def get_work_location_addresses(self, id):
        url = self._build_url(f"work-locations/{id}/addresses")
        return self.transmit(url)

    def get_employee_leave_balance(self, employee_id):
        params = {"employeeId": employee_id}
        url = self._build_url("employees/leave-balances") + "?" + urlencode(params)
        return self.transmit(url)

        # Legal Entity Leave Policy

    def get_legal_entity_leave_policy(self, id):
        url = self._build_url(f"legal-entities/{id}/leave-policies")
        return self.transmit(url)

        # Employee Time Off Requests

    def get_employee_time_off_requests(self, employee_id, status=None):
        params = {"employeeId": employee_id}
        if status:
            params["status"] = status

        url = self._build_url("employees/time-off-requests") + "?" + urlencode(params)
        return self.transmit(url)

    def create_employee_time_off_request(self, employee_id, data):
        params = {"employeeId": employee_id}
        url = self._build_url("employees/time-off-requests") + "?" + urlencode(params)
        return self.transmit(url, meta=data, mode='POST')


    def update_employee_time_off_request(self, request_id, data):
        url = self._build_url(f"employees/time-off-requests/{request_id}")
        return self.transmit(url, meta=data, mode='PATCH')

    def get_employee_hours_adjustments(self, employee_id):
        params = {"employeeId": employee_id}
        url = self._build_url("employees/hours-adjustments") + "?" + urlencode(params)
        return self.transmit(url)

    def create_employee_hours_adjustment(self, employee_id, data):
        params = {"employeeId": employee_id}
        url = self._build_url("employees/hours-adjustments") + "?" + urlencode(params)
        return self.transmit(url, meta=data, mode='POST')

    def get_employee_benefit_elections(self, employee_id):
        params = {"employeeId": employee_id}
        url = self._build_url("employees/benefit-elections") + "?" + urlencode(params)
        return self.transmit(url)

    def get_employee_pay_stub(self, employee_id, pay_number):
        params = {"employeeId": employee_id, "payNumber": pay_number}
        url = self._build_url("employees/pay-stubs") + "?" + urlencode(params)
        return self.transmit(url)

    def get_legal_entity_workers_compensation(self, id):
        url = self._build_url(f"legal-entities/{id}/workers-compensation")
        return self.transmit(url)

    def get_employee_time_tracking(self, employee_id):
        params = {"employeeId": employee_id}
        url = self._build_url("employees/time-tracking") + "?" + urlencode(params)
        return self.transmit(url)

    def get_employee_direct_deposit(self, employee_id):
        params = {"employeeId": employee_id}
        url = self._build_url("employees/direct-deposits") + "?" + urlencode(params)
        return self.transmit(url)

    def update_employee_direct_deposit(self, employee_id, data):
        params = {"employeeId": employee_id}
        url = self._build_url("employees/direct-deposits") + "?" + urlencode(params)
        return self.transmit(url, meta=data, mode='PATCH')

    def get_employee_emergency_contact(self, employee_id):
        params = {"employeeId": employee_id}
        url = self._build_url("employees/emergency-contacts") + "?" + urlencode(params)
        return self.transmit(url)

    def update_employee_emergency_contact(self, employee_id, data):
        params = {"employeeId": employee_id}
        url = self._build_url("employees/emergency-contacts") + "?" + urlencode(params)
        return self.transmit(url, meta=data, mode='PATCH')

    def get_legal_entity_onboarding(self, id):
        url = self._build_url(f"legal-entities/{id}/onboarding")
        return self.transmit(url)

    def update_legal_entity_onboarding(self, id, data):
        url = self._build_url(f"legal-entities/{id}/onboarding")
        return self.transmit(url, meta=data, mode='PATCH')

    def get_legal_entity_benefits(self, id):
        url = self._build_url(f"legal-entities/{id}/benefits")
        return self.transmit(url)

    def transmit(self, url, meta=None, mode='POST'):
        if meta:
            response = upool.request(mode, url, headers=self.header, fields=json.dc(meta))
        else:
            response = upool.request(mode, url, headers=self.header, fields={})
        print(response.status)
        if response.status == 200:
            return json.dc(response.data.decode())
        elif response.status == 429:
            # logger.info('waiting')
            print('waiting....')
            sleep(5)
            self.transmit(url, meta=meta or None, mode=mode)
        else:
            self.error = json.dc(response.data.decode())
            print(self.error)
            return self.error

if __name__=='__main__':
    print(API())