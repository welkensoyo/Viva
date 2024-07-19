import logging
import nfty.njson as j
import nfty.constants as constants
import arrow


logger = logging.getLogger("AppLogger")


class Tag:
    TAGID = ("x", "a", "i", "s", "k", "w")
    TAGKEY = ("admin", "all", "info", "system", "kyc", "workflow")
    TAGDICT = dict(zip(TAGID, TAGKEY))
    ADMIN_KEYS = ("x", "s", "k", "w")
    # example tags = {'x:partner':'cto', 'a:hash': '01/01/2023 00:00'}

    def __init__(self, entity):
        self.e = entity
        self.tz = self.timezone()
        self.tags = j.dc(self.e.tags, only=True)

    @classmethod
    def client(cls, client):
        return cls(client)

    @classmethod
    def document(cls, document):
        return cls(document)

    def timezone(self):
        try:
            return self.e.tz
        except Exception as e:
            logger.exception("Exception accessing timezone value")
            return constants.DEFAULT_TIMEZONE

    def now(self):
        return arrow.now(self.tz).format("YYYY/MM/DD/ HH:mm")

    def timedate(self, date):
        return arrow.get(date).to(self.tz).format("YYYY/MM/DD/ HH:mm")

    def listtags(self):
        return self.tags.keys()

    def check(self, tags):
        if isinstance(tags, list):
            for t in tags:
                if ":" not in t or t[0] not in self.TAGID:
                    return []
                return tags
        else:
            if ":" not in tags or tags[0] not in self.TAGID:
                return []
            return [
                tags,
            ]

    def filter(self, filter):
        return sorted(
            [
                i
                for i in self.listtags()
                if i[0] == self.TAGID[self.TAGKEY.index(filter)]
            ]
        )

    def all(self, filtered=None):
        if filtered:
            x = sorted(filtered.keys())
        else:
            x = sorted(list(self.listtags()))
        return [i for c in self.TAGID for i in x if i[0] == c]

    def clean_empty_tags(self):
        for each in self.tags.keys():
            if not self.tags[each]:
                del self.tags[each]

    def remove(self, tag):
        self.tags.pop(tag, None)
        return self.save()

    def add(self, tag, status=""):
        status = status or self.now()
        self.tags.save({tag: status})
        return self.save()

    def update(self, tags):
        self.tags = tags
        return self.save()

    def reset(self):
        self.tags = {}
        return self.save()

    def save(self):
        self.e.save({"tags": self.tags})
        return self


if __name__ == "__main__":
    from nfty.clients import Client

    t = Client("a086e12d-18ed-45c5-8b8a-81a634f19c8a").get_tags()
    print(t.tags)
    t.add("a:testapi", status="cto")
    t.add("x:testapi", status="admin")
    t.add("k:testapi")
    print(t.tags)
    print(t.all())
    t.remove("x:testapi")
    print(t.tags)
