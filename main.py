import json, time, sys, os
import logging, argparse
import logging.config

from requests.exceptions import Timeout, ConnectionError

from datetime import datetime, timedelta

from requests.auth import HTTPBasicAuth

from api_requests import TransactionDetail, ErrorTransactionListing, RequestUnsuccesfulError
from util import read_config, has_transaction, save_transaction, get_transaction_relative_path

import SocketServer
import BaseHTTPServer
import SimpleHTTPServer

import threading

logging.config.fileConfig('logging.conf')
#Setup logging
logging.basicConfig(level=logging.INFO)

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s %(message)s',  datefmt='%I:%M:%S %p')

ch.setFormatter(formatter)

logger = logging.getLogger(__name__)

has_http_server = False

def indexes_for_date(environment, dates):
  return set(["cc-proteus-%s.%s" % (environment, str(dt.isocalendar()[1]), ) for dt in dates])

def run(config):

  environment = config['environment']

  try:
    sleep_time = config['sleep_time']
  except KeyError: 
    sleep_time = 60

  tx_dir = os.path.join(config['logs_dir'], environment)

  if not os.path.exists(tx_dir):
    os.makedirs(tx_dir)

  while True:
    end = datetime.now()
    today = end.date()
    daterange = (datetime(today.year, today.month, today.day), end)
    date_folder = "%s.%s.%s" % (today.year, today.month, today.day)

    indexes = indexes_for_date(environment, daterange) 

    try :
      transactions = ErrorTransactionListing(indexes, daterange, config).run()

      for tx in transactions:
        if not has_transaction(tx_dir, date_folder, tx):
          logger.debug("Found a new error %s", tx)

          tx_url = None
          if has_http_server:
            tx_url = get_http_tx_url(config, date_folder, tx) 

          detail = TransactionDetail(indexes, tx, config, tx_url).run()

          save_transaction(tx_dir, date_folder, tx, detail)
    except Timeout, e:
      logger.warn("Timeout occured - retrying in a bit: %s", e)
    except ConnectionError, e:
      logger.warn("Connection error occured - retrying in a bit: %s", e)
    except RequestUnsuccesfulError, e:
      logger.warn("Response returned unhandled status code: %s", e)

    time.sleep(sleep_time)

def get_http_tx_url(config, date_folder, tx):
  return "http://%s:%s/%s" % (config['web_serve']['domain'],
                       config['web_serve']['port'],
                       get_transaction_relative_path(date_folder, tx))

# Simple HTTP Server which supports multiple requests..
class TransactionHTTPServer(SocketServer.ThreadingMixIn,
                            BaseHTTPServer.HTTPServer):
  pass

def get_transaction_server(tx_dir, serve_config):
  port = serve_config['port']

  os.chdir(tx_dir)
  s = TransactionHTTPServer(('', port), SimpleHTTPServer.SimpleHTTPRequestHandler)
  t = threading.Thread(target=s.serve_forever)

  t.setDaemon(True)

  return t

if __name__ == '__main__':
  parser = argparse.ArgumentParser(description="Proteus API error transaction saver (and broadcaster!)")
  parser.add_argument("--config", action='store', dest='config', help='location of configuration file to use (defaults to config.json)', default='config.json', type=open)
  args = parser.parse_args()

  with args.config as config_file:
    config = read_config(config_file)

  has_http_server = 'web_serve' in config

  try:
    if has_http_server:
      tx_dir = os.path.join(config['logs_dir'], config['environment'])
      serve = get_transaction_server(tx_dir, config['web_serve'])
      serve.start()
      print 'httpserver started'

    
    run(config)
  except KeyboardInterrupt:
    pass



