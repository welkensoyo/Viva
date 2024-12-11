# -*- coding: utf-8 -*-

import certifi
import urllib3
import logging


logger = logging.getLogger("AppLogger")

retries = urllib3.util.Retry(connect=5, read=3, redirect=2, backoff_factor=0.05)
upool = urllib3.PoolManager(
    cert_reqs="CERT_REQUIRED",
    ca_certs=certifi.where(),
    num_pools=20,
    block=False,
    retries=retries,
)
apple_pool = urllib3.PoolManager(
    cert_reqs="CERT_REQUIRED",
    ca_certs=certifi.where(),
    cert_file="keys/apple_mid.crt.pem",
    key_file="keys/apple_mid.key.pem",
    num_pools=7,
    block=False,
)

