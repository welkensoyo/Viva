from quickbooks import QuickBooks, QuickBooksReport


class QuickbooksClient:
    def __init__(self):
        self.client = QuickBooks(
            sandbox=True,
            consumer_key="CONSUMER_KEY",
            consumer_secret="CONSUMER_SECRET",
            access_token="ACCESS_TOKEN",
            access_token_secret="ACCESS_TOKEN_SECRET",
            company_id="COMPANY_ID"
        )

    def get_company_info(self):
        return self.client.get_company_info()

    def get_report(self, report_type):
        report = QuickBooksReport.get_report(
            client=self.client,
            report_type=report_type
        )
        return report

    def get_accounts_receivable_report(self):
        return self.get_report("AgedReceivables")

    def get_accounts_payable_report(self):
        return self.get_report("AgedPayables")