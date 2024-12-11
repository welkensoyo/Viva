import logging
import traceback
import schedule
import arrow
from gevent import sleep, spawn as spawn


logger = logging.getLogger("AppLogger")
table = "nfty.scheduler"
users_table = 'users.chat'

qry = {
    "next": f"""
        UPDATE {table} SET status = 'processing' WHERE id IN ( 
        SELECT id FROM nfty.subscriptions 
        WHERE meta->>'next' <= %s AND status = 'true'
        FOR UPDATE SKIP LOCKED limit 1
    ) RETURNING id, clientid 
""",
}


def start_thread() -> None:
    """
    This function kicks off a thread, optionally leveraging a test path if the "TEST_SUBSCRIPTION_PAYMENTS" environment variable is provided.

    Returns:
        None
    """
    set_schedule()
    while 1:
        try:
            schedule.run_pending()
        except Exception as e:
            traceback.print_exc()
        sleep(5)


def start():
    # spawn(spark_sync)
    spawn(dumpsterfire_sync)
    return spawn(start_thread)


def set_schedule() -> None:
    """
    Sets up a schedule for success

    Returns:
        None
    """
    if env == constants.PRODUCTION:
        pass
    logger.info("Scheduler Started...")

def dumpsterfire_sync():
    """ This checks postgres for changes in the database and updates the user websocket if exists """
    from nfty.events import dumpsterfire_sync
    from nfty.db import _pgcon
    conn = _pgcon().create_connection()
    cur = conn.cursor()
    cur.execute("LISTEN dumpsterfire;")
    while 1:
        conn.poll()
        while conn.notifies:
            message = conn.notifies.pop(0).payload
            for x in dumpsterfire_sync(message):
                if x in ws_connections:
                    ws_connections[x]['ws'].send(message)
            sleep(0)
        sleep(0.5)


