import logging
import telnyx
from api._config import telnyx as api_key

logger = logging.getLogger("AppLogger")


def send(to, message):
    logger.info(f"Sending SMS message: {message} to: {to}")
    telnyx.api_key = api_key.get("key")
    telnyx_number = "19043029887"
    return telnyx.Message.create(
        from_=telnyx_number,
        to=to,
        text=message,
    )
