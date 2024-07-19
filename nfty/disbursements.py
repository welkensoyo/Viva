import logging
import nfty.log as log
from nfty.clients import Client
from nfty.processors.xpress import API as xe
import arrow


logger = logging.getLogger("AppLogger")


class API:
    def __init__(self, mode="current"):
        if mode == "current":
            self.details = log.Logs().revenue("4a731b24-0beb-457c-9585-2362c6a53cf5")
        if mode == "all":
            self.details = log.Logs().revenue_all(
                "4a731b24-0beb-457c-9585-2362c6a53cf5"
            )

    def payouts(self):
        parents = {}
        for each in self.details:
            if each[0] in ("4a731b24-0beb-457c-9585-2362c6a53cf5", "TriplePlayPay"):
                continue
            if each[0] not in parents:
                parents[each[0]] = float(each[-4] or 0)
            else:
                parents[each[0]] += float(each[-4] or 0)
        return parents

    def client_fees(self):
        parents = {}
        for each in self.details:
            if each[0] == "4a731b24-0beb-457c-9585-2362c6a53cf5":
                continue
            if each[0] not in parents:
                parents[each[0]] = float(each[-3] or 0)
            else:
                parents[each[0]] += float(each[-3] or 0)
        return parents

    def collect(self):
        date = arrow.get().shift(days=-1).format("YYYY-MM-DD")
        for each in self.details:
            if each[1] == "4a731b24-0beb-457c-9585-2362c6a53cf5":
                continue
            x = xe(Client(each[1]), None)
            if float(each[-1]):
                x.collect(f"{float(each[-1]):.2f}", date)
        return
