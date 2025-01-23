from gevent import sleep
import arrow
from nfty.communications import upool
import nfty.njson as json
import urllib3
from urllib.parse import urlencode
from api._config import paycor as config
import nfty.db as db

access = 'eyJhbGciOiJSUzI1NiIsImtpZCI6IjAzNGNkMjVjNDI4ZDFkOTRkM2MzMWY2NGM4NzM1YjA5IiwidHlwIjoiSldUIn0.eyJuYmYiOjE3MzQxMDY3ODAsImV4cCI6MTczNDExMjE4MCwiaXNzIjoiaHR0cHM6Ly9hcGkucGF5Y29yLmNvbS9zdHMvdjIvY29tbW9uIiwiYXVkIjoiY2JlMDk3YWU0MThiMGQ4YjE4NTAiLCJjbGllbnRfaWQiOiJjYmUwOTdhZTQxOGIwZDhiMTg1MCIsInNrZXkiOiJMRWxLeEFBSjlHNkVMUjlCUG9iN0RxLUVFMFhlMFNHellxcXFZbXgtUmIwIiwic2lkIjoiNDZjMjM1NzktZDM2NC00MjdmLTg0MjItM2MxNzM3YmI2ODA2IiwiYXV0aF90aW1lIjoxNzM0MTA5NjIyLCJ2ZXIiOjEsInN1YiI6ImZhZjljYmY1LTU2NzYtNGExYi05OGU4LTNjMzI1MmUwZjFkNyIsImlkcCI6MywicGF5Y29yX3VzZXJpZGVudGl0eSI6IjAwMDAwMDAwLTAwMDAtMDAwMC0wMDAwLTAwMDAwMDAwMDAwMCIsInBheWNvcl90aW1lb3V0IjoyMCwiaXBhZGRyIjoiMTA3LjEyOC4xOTkuMTY5IiwianRpIjoiZTE4YzNhMWYyYzYyMzA5MDA4YWNmZjk0ZjFlNTViMTkiLCJzY29wZSI6WyIzN2Q1OTBhYTc1YjllZjExODhjZjAwMjI0ODhkN2U1NiIsIm9mZmxpbmVfYWNjZXNzIl0sImFtciI6WyJwd2QiXX0.hpGlRUbO-tzbiMxS10l7Q2b5AUOc0d_xsrbIY9u4jJ8fyP_F-4U-6Zbc3JgR_K60JgucJqBciRZz5baBXxx1urmbnngyYWovjbVaDcIF90YKhRgeUBxzw09HsbjhmdI7RMaGzR8Gcy7tjSBjVwk1YoG0Yo_kphkjDqJM_TtvS1IPOVWicgUldfStAEugAcZ64-eu-By0__rVdVTeTAiSa6epE3zC6I3jG90qNkfHpVp7S_OJTbL86bJVpgwpUQ7Cr_vP1nFEI77bf4mFpV5TNIbEN4BlIfghpKwGITaesU3sNm5vNwgUpuF_zUW6tA5tCJav-Mpds6Wx6f4kvcJACw'
refresh = 'b83a323fc23a8d26ce8e100b83e1633da571c176a317be74cbc5c45149cdd2fa'

qry = {
    'token' : "SELECT token_data FROM users.tokens WHERE id = '1' ",
    'upsert': "INSERT INTO users.tokens (id, token_data) VALUES ('1', %s) ON CONFLICT (id) DO UPDATE SET token_data = %s"
}

class API:
    client_id = config.client_id
    company_id = config.company_id

    def __init__(self):
        self.error = None
        self.pre_url = 'https://apis.paycor.com/v1/'
        self.header = {
            'Content-Type': "application/json",
            'Ocp-Apim-Subscription-Key': '94624d3fedee456b86d51fdc7f89be9a'
        }
        self.token = ''
        self.auth()

    def auth(self):
        meta = json.dc(db.fetchreturn(qry['token']))
        print(meta)
        if not meta:
            self.token = access
            refresh = 'b83a323fc23a8d26ce8e100b83e1633da571c176a317be74cbc5c45149cdd2fa'

        url = 'https://apis.paycor.com/sts/v1/common/token?subscription-key=94624d3fedee456b86d51fdc7f89be9a'
        header = {
            'grant_type' : 'refresh_token',
            'refresh_token' : refresh,
            'client_id' : config.client_id,
            'client_secret' : config.secret,
        }
        print(header)
        x = upool.request('POST', url, headers=header)
        print(x.status)
        print(x.data.decode())
        x = json.dc(x.data.decode())
        if 'access_token' in x:
            db.execute(qry['upsert'], x.data.decode())
            self.token = 'Bearer '+x['access_token']
            self.header['Authorization'] = self.token
            return True
        else:
            return False

    def session(self):
        if not self.token:
            url = 'https://apis.paycor.com/sts/v1/common/token?subscription-key=94624d3fedee456b86d51fdc7f89be9a'
            x = upool.request('POST', url)
            print(x.status)
            print(x.data)
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

    def persons(self):
        url = self._build_url(f"tenants/{self.client_id}/persons")
        return self.transmit(url, mode='GET')

    def accounts(self):
        url = self._build_url(f"legalEntities/{self.company_id}/ats/accounts")
        return self.transmit(url, mode='GET')

    def update_legal_entity_work_location(self, id, data):
        url = self._build_url(f"legalIEntities/{id}/work-locations")
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
        url = self._build_url(f"legalIEntities/{id}/leave-policies")
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
        url = self._build_url(f"legalIEntities/{id}/workers-compensation")
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
        url = self._build_url(f"legalIEntities/{id}/onboarding")
        return self.transmit(url)

    def update_legal_entity_onboarding(self, id, data):
        url = self._build_url(f"legalIEntities/{id}/onboarding")
        return self.transmit(url, meta=data, mode='PATCH')

    def get_legal_entity_benefits(self, id):
        url = self._build_url(f"legalIEntities/{id}/benefits")
        return self.transmit(url)

    def transmit(self, url, meta=None, mode='POST'):
        print(url)
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
    print(API().accounts())