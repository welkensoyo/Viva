import logging
import arrow
import nfty.db as db


logger = logging.getLogger("AppLogger")

localtime = arrow.now("US/Central")

qry = {
    "search": """ SELECT name,abbrev,utc_offset,is_dst FROM pg_timezone_names WHERE name like %s """,
    "all": """ SELECT name,abbrev,utc_offset,is_dst FROM pg_timezone_names """,
}

states = {
    "AK": "Alaska",
    "AL": "Alabama",
    "AR": "Arkansas",
    "AZ": "Arizona",
    "CA": "California",
    "CO": "Colorado",
    "CT": "Connecticut",
    "DC": "District of Columbia",
    "DE": "Delaware",
    "FL": "Florida",
    "GA": "Georgia",
    "HI": "Hawaii",
    "IA": "Iowa",
    "ID": "Idaho",
    "IL": "Illinois",
    "IN": "Indiana",
    "KS": "Kansas",
    "KY": "Kentucky",
    "LA": "Louisiana",
    "MA": "Massachusetts",
    "MD": "Maryland",
    "ME": "Maine",
    "MI": "Michigan",
    "MN": "Minnesota",
    "MO": "Missouri",
    "MS": "Mississippi",
    "MT": "Montana",
    "NC": "North Carolina",
    "ND": "North Dakota",
    "NE": "Nebraska",
    "NH": "New Hampshire",
    "NJ": "New Jersey",
    "NM": "New Mexico",
    "NV": "Nevada",
    "NY": "New York",
    "OH": "Ohio",
    "OK": "Oklahoma",
    "OR": "Oregon",
    "PA": "Pennsylvania",
    "RI": "Rhode Island",
    "SC": "South Carolina",
    "SD": "South Dakota",
    "TN": "Tennessee",
    "TX": "Texas",
    "UT": "Utah",
    "VA": "Virginia",
    "VT": "Vermont",
    "WA": "Washington",
    "WI": "Wisconsin",
    "WV": "West Virginia",
    "WY": "Wyoming",
}


def timezones(search=None):
    if not search:
        return db.fetchall(qry["all"])
    return db.fetchall(qry["search"], f"{search}/%")


class ZipTime(object):
    TICKET_DATETIME_FORMAT = "YYYYMMDD_HHmmss_SSSSSS"

    @classmethod
    def pg(cls, zipcode):
        PSQL = """ SELECT city, county, state, country, tz, dst, salestax FROM nfty.client.timezonebyzipcode WHERE zip = %s """
        tzinfo = db.Pcursor().fetchone(PSQL, zipcode)
        meta = {
            "zipcode": zipcode,
            "city": tzinfo[0],
            "county": tzinfo[1],
            "state": tzinfo[2],
            "country": tzinfo[3],
            "timezone": tzinfo[4],
            "dst": tzinfo[5],
            "salestax": tzinfo[6],
        }
        return cls(meta)

    @classmethod
    def sl(cls, zipcode):
        import sqlite3

        SQL = """ SELECT city, county, state, country, tz, dst, salestax FROM tz WHERE zip = ? """
        cursor = sqlite3.connect("tz.db").cursor()
        cursor.execute(SQL, (zipcode,))
        tzinfo = cursor.fetchone()
        meta = {
            "zipcode": zipcode,
            "city": tzinfo[0],
            "county": tzinfo[1],
            "state": tzinfo[2],
            "country": tzinfo[3],
            "timezone": tzinfo[4],
            "dst": tzinfo[5],
            "salestax": tzinfo[6],
        }
        return cls(meta)

    @classmethod
    def cached(cls, hashcode, zipcode=None):
        from nfty.redis_cache import cache

        meta = cache.meta(hashcode, zipcode)
        return cls(meta)

    def __init__(self, meta):
        self.meta = meta
        if "zipcode" in meta:
            self.zipcode = str(self.meta["zipcode"])
        else:
            self.zipcode = "37203"  # Nashville in case of new build
        if (
            not self.meta.get("timezone")
            or not self.meta.get("dst")
            or not self.meta.get("salestax")
        ):
            self.get_sql()

    def get(self):
        return self.meta

    def get_sql(self):
        PSQL = """ SELECT city, county, state, country, tz, dst, salestax FROM client.timezonebyzipcode WHERE zip = %s """
        tzinfo = db.Pcursor().fetchone(PSQL, self.zipcode)
        self.meta.save(
            {
                "zipcode": self.zipcode,
                "city": tzinfo[0],
                "county": tzinfo[1],
                "state": tzinfo[2],
                "country": tzinfo[3],
                "timezone": tzinfo[4],
                "dst": tzinfo[5],
                "salestax": tzinfo[6],
            }
        )

    def local(self):
        return arrow.utcnow().to(self.meta["timezone"])

    def __str__(self):
        return self.local().format(self.TICKET_DATETIME_FORMAT)

    def str(self, xdate=None):
        if not xdate:
            return self.local().format(self.TICKET_DATETIME_FORMAT)
        return (
            arrow.get(xdate)
            .to(self.meta["timezone"])
            .format(self.TICKET_DATETIME_FORMAT)
        )

    def print_sql(self):
        return self.local().format("YYYY-MM-DD")

    def print_date(self, xdate):
        if not xdate:
            return self.local().format("YYYY-MM-DD HH:mm:ss")
        return arrow.get(xdate).format("YYYY-MM-DD HH:mm:ss")

    def print_time(self, xdate=None):
        if not xdate:
            return self.local().format("hh:mm:ss a")
        return arrow.get(xdate).format("hh:mm:ss a")

    def print_24time(self, xdate):
        if not xdate:
            return self.local().format("HH:mm:ss")
        return arrow.get(xdate).format("HH:mm:ss")

    def timediff(self, xdate):
        if isinstance(xdate, str):
            xdate = self.fromstr(xdate)
        if isinstance(xdate, (int, float)):
            xdate = self.fromstr(str(xdate))
        xdiff = self.now() - xdate
        return xdiff.seconds

    def time2java(self, xdate=None):
        if not xdate:
            xdate = self.now()
        else:
            xdate = self.local()
        return str(xdate.timestamp) + str(xdate.microsecond)

    def java2time(self, javatime):
        return arrow.get(str(javatime)).datetime

    def taxrate(self):
        return self.meta["salestax"]

    def salestax(self):
        return self.taxrate()

    def timezone(self):
        return self.meta["timezone"]

    def dst(self):
        return self.meta["dst"]

    def hashitems(self):
        result = {}
        for dictionary in self.meta["itemhashlist"]:
            result.update(self.meta["itemhashlist"][dictionary])
        return result

    @staticmethod
    def fromstrz(xdate, timezone="US/Central"):
        return arrow.get(xdate, ZipTime.TICKET_DATETIME_FORMAT).replace(tzinfo=timezone)

    @staticmethod
    def fromstr(xdate, timezone=None):
        return arrow.get(xdate, ZipTime.TICKET_DATETIME_FORMAT)

    @staticmethod
    def fromstrprint(xstr):
        return arrow.get(xstr, "YYYYMMDD_HHmmss_SSSSSS").format("hh:mm:ss a")

    @staticmethod
    def rating_date(xdate=arrow.utcnow()):
        return arrow.get(xdate, ZipTime.TICKET_DATETIME_FORMAT).format("M/D/YY")

    @staticmethod
    def zip2timezone(zipcode):
        import nfty.timezones as nz

        if zipcode:
            zipcode = int(zipcode)
            for i in nz.STATE_ZIPCODE_RANGES:
                if zipcode >= i[0] and zipcode <= i[1]:
                    if i[2] in nz.STATE_WITH_FIXED_TIMEZONE:
                        return nz.STATE_WITH_FIXED_TIMEZONE[i[2]]["timezone"], True
                    else:
                        PSQL = """ SELECT tz, dst FROM client.zip2tz WHERE zip = %s"""
                        return db.Pcursor().fetchone(PSQL, str(zipcode))
        return "US/Central", True

    @staticmethod
    def ziptolocal(xdate, zipcode):
        tzone, dst = ZipTime.zip2timezone(zipcode)
        if not xdate:
            return arrow.utcnow().to(tzone)
        return arrow.get(xdate).to(tzone)

    @staticmethod
    def now(xdate=arrow.utcnow()):
        return arrow.get(xdate).datetime

    @staticmethod
    def tolocal(timezone):
        return arrow.utcnow().to(timezone)

    @staticmethod
    def toarrow(xdate=arrow.utcnow()):
        return arrow.get(xdate)

    @staticmethod
    def monthlast(xdate=arrow.utcnow()):
        return arrow.get(xdate).ceil("month").datetime

    @staticmethod
    def monthfirst(xdate=arrow.utcnow()):
        return arrow.get(xdate).floor("month").datetime

    @staticmethod
    def monthrange(xdate=arrow.utcnow()):
        return (
            arrow.get(xdate).floor("month").datetime,
            arrow.get(xdate).ceil("month").datetime,
        )

    @staticmethod
    def timer():
        import sys

        global localtimeCST
        x = arrow.now("US/Central")
        xdiff = x - localtimeCST
        localtimeCST = x
        print("")
        print((sys._getframe().f_back.f_code))
        print((sys._getframe().f_back.f_code.co_name))
        print(x)
        print((xdiff.seconds, " seconds"))
        print("")
        return xdiff.seconds

    @staticmethod
    def translatemonth(month):
        months = {
            "JAN": 1,
            "FEB": 2,
            "MAR": 3,
            "APR": 4,
            "MAY": 5,
            "JUN": 6,
            "JUL": 7,
            "AUG": 8,
            "SEP": 9,
            "OCT": 10,
            "NOV": 11,
            "DEC": 12,
        }
        x = arrow.now("US/Central")
        if month:
            cur = months[month]
            if x.month != month:
                x = arrow.get(f"{x.year}-{cur}-1")
        return x.format("YYYY-MM-DD hh:mm:ss")
