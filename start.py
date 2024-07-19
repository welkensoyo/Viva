from gevent import monkey, spawn, sleep
from gevent import signal_handler as sig
monkey.patch_all()
import signal
import os
import bottle
import nfty.scheduler as scheduler
import logging
import sys
from gevent.pywsgi import WSGIServer
from geventwebsocket.handler import WebSocketHandler
from beaker.middleware import SessionMiddleware
from mainapp import mainappRoute
from customRoutes import cRoute
from api._config import beakerconfig, systemoptions, PORT
from whitenoise import WhiteNoise, compress
from nfty.constants import IGNORE_COMPRESS_FILE_TYPES

from gevent.ssl import SSLContext, PROTOCOL_TLS_SERVER

ssl_context = SSLContext(PROTOCOL_TLS_SERVER)
ssl_context.load_cert_chain(certfile='keys/server.crt', keyfile='keys/server.key')

debug = False
staticfolder = "static"
bottle.TEMPLATE_PATH.insert(0, "templates/")
bottle.TEMPLATE_PATH.insert(0, "templates/render/")

if os.getenv("LOG_LEVEL"):
    level = logging.getLevelName(int(os.getenv("LOG_LEVEL")))
else:
    level = logging.getLevelName(10)
logger = logging.getLogger("ApiLogger")
logger.setLevel(level)
format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s", datefmt="%m/%d/%Y %I:%M:%S %p",)
ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(format)
logger.addHandler(ch)


def check_compress():
    c = compress.Compressor(extensions=IGNORE_COMPRESS_FILE_TYPES)
    ds = ("static", "dist")
    for d in ds:
        for dirpath, _dirs, files in os.walk(d):
            for filename in files:
                sleep(0)
                if c.should_compress(filename):
                    if not os.path.exists(dirpath + "/" + filename + ".br"):
                        for each in c.compress(os.path.join(dirpath, filename)):
                            print(each)
    logger.debug("FILE COMPRESSION COMPLETED")


if __name__ == "__main__":
    if systemoptions.get("compress"):
        logger.debug("App Starting - Compressing Files")
        spawn(check_compress)

    logger.info(f"Application Starting on port {PORT}")
    botapp = bottle.app()
    for Route in (mainappRoute, cRoute):
        botapp.merge(Route)
    botapp = SessionMiddleware(botapp, beakerconfig)
    botapp = WhiteNoise(botapp)
    botapp.add_files(staticfolder, prefix="static/")
    botapp.add_files("dist", prefix="dist/")
    logger.info("Scheduler starting")
    scheduler.start()
    logger.info("Scheduler started")
    logger.info("Server starting")
    server = WSGIServer(("0.0.0.0", int(PORT)), botapp, ssl_context=ssl_context, handler_class=WebSocketHandler)
    logger.info("Server Started")

    def shutdown():
        logger.critical("SYSTEM SHUTTING DOWN")
        server.stop(timeout=5)
        exit(signal.SIGTERM)

    sig(signal.SIGTERM, shutdown)
    sig(signal.SIGINT, shutdown)
    server.serve_forever()


# bottle.py was changed to use underscore instead of dash for blanks in file name.  Updates will need this change readded.

""" #Service on Linux to run python
#! /bin/sh
# /etc/init.d/pythonsvc
 
case "$1" in
  start)
    echo "Starting App Service via opt/wwww/start.py"
    # run application you want to start
    cd /
    cd /opt/www
    /root/miniconda2/bin/python start.py &
    ;;
  stop)
    echo "Stopping App Service"
    # kill application you want to stop
    pkill -9 python
    ;;
  *)
    echo "Usage: /etc/init.d/wllsvc{start|stop}"
    exit 1
    ;;
esac 
"""

""" COPY SCRIPT
cd /opt
rm -R www
mv /tmp/www /opt
sudo /etc/init.d/wllsvc.sh stop
sudo /etc/init.d/wllsvc.sh start

http://support.worldpay.com/support/CNP-API/content/dpayfacdp.htm
"""
