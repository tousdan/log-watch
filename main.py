import json, time, sys, os
import logging, argparse
import logging.config

from requests.exceptions import Timeout, ConnectionError

from datetime import datetime, timedelta

from requests.auth import HTTPBasicAuth

from api_requests import TransactionDetail, ErrorTransactionListing, RequestUnsuccesfulError

logging.config.fileConfig('logging.conf')
#Setup logging
logging.basicConfig(level=logging.INFO)

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s %(message)s',  datefmt='%I:%M:%S %p')

ch.setFormatter(formatter)

logger = logging.getLogger(__name__)


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

          detail = TransactionDetail(indexes, tx, config).run()

          save_transaction(tx_dir, date_folder, tx, detail)
    except Timeout, e:
      logger.warn("Timeout occured - retrying in a bit: %s", e)
    except ConnectionError, e:
      logger.warn("Connection error occured - retrying in a bit: %s", e)
    except RequestUnsuccesfulError, e:
      logger.warn("Response returned unhandled status code: %s", e)


    time.sleep(sleep_time)

def has_transaction(tx_dir, date_folder, transactionId):
  return os.path.isfile(os.path.join(tx_dir, date_folder, "%s.json" % (transactionId,)))

def save_transaction(tx_dir, date_folder, transactionId, detail):
  target = os.path.join(tx_dir, date_folder)

  if not os.path.exists(target):
    os.makedirs(target)

  path = os.path.join(target, "%s.json" % (transactionId,))

  logger.debug("Saving error detail to %s results", path)
  with open(path, "w") as output:
    output.write(json.dumps(detail, sort_keys=True, indent=4, separators=(',', ': ')))

def read_config(config_file):
  config = json.loads(config_file.read())

  for key in ['username', 'password', 'environment', 'es_url', 'logs_dir']:
    if not key in config:
      raise NameError(key)
  
  return config

if __name__ == '__main__':
  parser = argparse.ArgumentParser(description="Proteus API error transaction saver (and broadcaster!)")
  parser.add_argument("--config", action='store', dest='config', help='location of configuration file to use (defaults to config.json)', default='config.json', type=open)
  args = parser.parse_args()

  with args.config as config_file:
    config = read_config(config_file)

  try:
    run(config)
  except KeyboardInterrupt:
    pass

