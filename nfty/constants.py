# User Roles
ADMIN = "admin"
USER = "user"

# Environments
PRODUCTION = "prod"
LOCALHOST = "127.0.0.1"
SANDBOX = "sandbox"
QA = "qa"

# Locations
COUNTRY_DEFAULT = "US"
ZIPCODE_TEST_CLIENT = "37067"

# Times
END_OF_DAY = "20:00:00"

# Default Values
DEFAULT_TIMEZONE = "US/Central"
DEFAULT_OWNERSHIP_TYPE = "LLC Private"
DEFAULT_BUSINESS_TYPE = "ECommerce"
DEFAULT_ORIGIN = "portal"
DEFAULT_ZIPCODE = 0
DEFAULT_DATE_FORMAT = "YYYY-MM-DD"
DEFAULT_END_DATE = "9999-01-01"
CHECK_API_SPECIFICATIONS = 'Oops... Please check API specifications and try again... '
EMAIL_DEV_TEAM = "djbartron@gmail.com"
EMAIL_TEAM = "djbartron@gmail.com"
ENTITY_TABLE = 'entities'
USER_TABLE = 'users'
ROOT_URL = "https://www.dumpsterfire.love"
RECAPTCHA_KEY = ''
IGNORE_COMPRESS_FILE_TYPES = [
    "pdf",
    "jpg",
    "jpeg",
    "png",
    "gif",
    "7z",
    "zip",
    "gz",
    "tgz",
    "bz2",
    "tbz",
    "xz",
    "br",
    "swf",
    "flv",
    "woff",
    "woff2",
    "eot",
    "py",
    "pdf",
    "docx",
    "svg",
    "ttf",
    "otf",
]

MB_PERSONALITIES = {
    "ISTJ": ["ISFJ", "ESTJ", "ISTP", "INTJ"],
    "ISFJ": ["ISTJ", "ESFJ", "ISFP", "INFJ"],
    "INFJ": ["ENFJ", "INFP", "INFJ", "INTJ"],
    "INTJ": ["ENTJ", "INTP", "INTJ", "ISTJ"],
    "ISTP": ["ESTP", "ISTJ", "ISTP", "ISFP"],
    "ISFP": ["ESFP", "ISFJ", "ISFP", "INFP"],
    "INFP": ["ENFP", "INFJ", "INFP", "ISFP"],
    "INTP": ["ENTP", "INTJ", "INTP", "INFP"],
    "ESTP": ["ESTP", "ESFP", "ISTP", "ENTP"],
    "ESFP": ["ESFP", "ESFJ", "ISFP", "ESTP"],
    "ENFP": ["ENFP", "ENFJ", "INFP", "INFJ"],
    "ENTP": ["ENTP", "ENTJ", "INTP", "ENFP"],
    "ESTJ": ["ESTJ", "ESFJ", "ISTJ", "ENTJ"],
    "ESFJ": ["ESFJ", "ESFP", "ISFJ", "ESTJ"],
    "ENFJ": ["ENFJ", "ENFP", "INFJ", "INFP"],
    "ENTJ": ["ENTJ", "ENTP", "INTJ", "ESTJ"]
}