import logging
from nfty.njson import jc, dc
import plaid
from plaid.api import plaid_api
from plaid.model.link_token_create_request import LinkTokenCreateRequest
from plaid.model.link_token_create_request_user import LinkTokenCreateRequestUser
from plaid.model.products import Products
from plaid.model.country_code import CountryCode
from plaid.model.auth_get_request import AuthGetRequest
from plaid.model.identity_get_request import IdentityGetRequest
from api._config import env, plaid_keys


logger = logging.getLogger("AppLogger")


if (
    env == "prod"
):  # {'{"public_token":"public-production-e3cb8f8e-27df-4efd-b585-d3999705b6ef"}': ''}
    configuration = plaid.Configuration(
        host=plaid.Environment.Production,
        api_key={
            "clientId": plaid_keys.client,
            "secret": plaid_keys.secret,
        },
    )
else:
    configuration = plaid.Configuration(
        host=plaid.Environment.Sandbox,
        api_key={
            "clientId": "64c158a3f02dfa0018b3cfeb",
            "secret": "601afe521f22575811dd5fc5bdd14d",
        },
    )


class API:
    def __init__(self, client):
        self.c = client
        self.api_client = plaid.ApiClient(configuration)
        self.plaid_client = plaid_api.PlaidApi(self.api_client)

    def authorization(self, access_token):
        request = AuthGetRequest(access_token=access_token)
        return self.plaid_client.auth_get(request)

    def identity(self, access_token):
        request = IdentityGetRequest(access_token=access_token)
        return self.plaid_client.identity_get(request)

    def link_token(self):
        client_user_id = self.c.id
        # Create a link_token for the given user
        request = LinkTokenCreateRequest(
            products=[Products("auth")],
            client_name=self.c.parent.get("name", "Triple Play Pay"),
            country_codes=[CountryCode("US")],
            # redirect_uri='https://www.tripleplaypay.com/app',
            language="en",
            webhook=f"https://tripleplaypay.com/callback/plaid/{self.c.id}",
            # redirect_uri='https://www.tripleplaypay.com/plaid/oauth.html',
            user=LinkTokenCreateRequestUser(client_user_id=client_user_id),
        )
        response = self.plaid_client.link_token_create(request)
        return jc(response.to_dict())

    def access_token(self, token):
        from plaid.model.item_public_token_exchange_request import (
            ItemPublicTokenExchangeRequest,
        )

        # the public token is received from Plaid Link
        exchange_request = ItemPublicTokenExchangeRequest(public_token=token)
        exchange_response = self.plaid_client.item_public_token_exchange(
            exchange_request
        )
        self.access_token = exchange_response["access_token"]
        return self.access_token

    def asset(self, token):
        from plaid.model.asset_report_pdf_get_request import AssetReportPDFGetRequest

        pdf_request = AssetReportPDFGetRequest(asset_report_token=token)
        pdf = self.plaid_client.asset_report_pdf_get(pdf_request)
        FILE = open("asset_report.pdf", "wb")
        FILE.write(pdf.read())
        FILE.close()

    def save(self, body):
        self.c.keystore["plaid"] = dc(body).get("public_token")
        self.c.save({})
        return self

    def transactions(self):
        from plaid.model.transactions_sync_request import TransactionsSyncRequest

        request = TransactionsSyncRequest(
            access_token=self.access_token,
        )
        response = self.plaid_client.transactions_sync(request)
        transactions = response["added"]

        # the transactions in the response are paginated, so make multiple calls while incrementing the cursor to
        # retrieve all transactions
        while response["has_more"]:
            request = TransactionsSyncRequest(
                access_token=self.access_token, cursor=response["next_cursor"]
            )
            response = self.plaid_client.transactions_sync(request)
            transactions += response["added"]


if __name__ == "__main__":
    from nfty.clients import Client

    pb = "public-production-e3cb8f8e-27df-4efd-b585-d3999705b6ef"
    api = API(Client("5babb2ff-8e5e-4b7f-92a9-9f36453f6594")).save(
        '{"public_token":"' + pb + '"}'
    )
    ac = "access-production-5c9baba5-e738-44cc-b6e1-2e8a92e3edcd"
    # print(nfty.identity(ac))
