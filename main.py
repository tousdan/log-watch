import requests, json
import pprint, time
import sys, os
import logging
import doctest
from datetime import datetime, timedelta

from requests.auth import HTTPBasicAuth

#Setup logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

handler = logging.StreamHandler()
handler.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s %(message)s',  datefmt='%I:%M:%S %p')
handler.setFormatter(formatter)
logger.addHandler(handler)

pp = pprint.PrettyPrinter(indent=2)

def run(config):
  environment = config['environment']
  
  try:
    sleep_time = config['sleep_time']
  except KeyError: 
    sleep_time = 60

  tx_dir = "C:/temp/transactions/%s" % (environment, )

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
    except requests.exceptions.TimeoutError, e:
      logger.warn("Timeout occured - retrying in a bit: %s", e)
    except requests.exceptions.ConnectionError, e:
      logger.warn("Connection error occured - retrying in a bit: %s", e)

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

def shards_failed(response):
  return response['_shards']['failed'] > 0

def indexes_for_date(environment, dates):
  return set(["cc-proteus-%s.%s" % (environment, str(dt.isocalendar()[1]), ) for dt in dates])

def totimestamp(dt):
  """Converts datetime to timestamp

  >>> totimestamp(datetime(2015, 02, 11))
  1423630800000L

  >>> totimestamp(datetime(2015, 02, 11, 23, 59, 59))
  1423717199000L
  """

  return int(time.mktime(dt.timetuple())) * 1000 

class Request(object):

  def __init__(self, indexes, request_data, config):
    self.indexes = indexes
    self.request_data = request_data
    self.config = config

  def run(self):
    response = requests.post("%s%s/_search" % (config['es_url'], ",".join(self.indexes)), 
                            auth=HTTPBasicAuth(config['username'], config['password']), 
                            data=json.dumps(self.request_data))
    
    logger.debug("Launching request")
    if response.status_code == 200:
      json_response = response.json()
      if shards_failed(json_response):
        logger.error("Some shards failed to respond")

      page_result = json_response['hits']
      
      return self.parse(page_result['hits'])
    else:
      logger.error("Request returned an error %s : \n%s", response.status_code, response.text)

      return None

  def read_field(self, entry, fieldname):
    if fieldname in entry['fields']:
      return entry['fields'][fieldname][0]

    return None

  def read_fields(self, entry, fieldnames):
    return tuple([self.read_field(entry, field) for field in fieldnames])

  def parse(self, hits):
    return hits


class TransactionDetail(Request):
  def __init__(self, indexes, transactionId, config):
    self.transactionId = transactionId
    request_data = {
       "sort": "@timestamp",
       "fields": ["@timestamp", "loggerName", "message", "stacktrace", "transactionId"],
       "query": {
        "filtered": {
          "query": {
            "bool": {
              "should": [
                {"query_string": { "query": "transactionId:\"%s\"" % (transactionId, ) } }
              ]
            }
          }
        }
      }
    }

    Request.__init__(self, indexes, request_data, config)

  def parse(self, hits):
    result = []
    logger.debug("Fetching transaction detail for %s", self.transactionId)
    for entry in hits:
      tstamp, tx, className, msg, stack = self.read_fields(entry, ('@timestamp', 'transactionId', 'loggerName', 'message', 'stacktrace'))

      result.append({
        'transaction': tx,
        'time': tstamp,
        'class':  className,
        'message': msg,
        'stack': stack,
      })

    logger.info("%s -> %s", self.transactionId, result[-1]['message'][:50])

    return result


class ErrorTransactionListing(Request):
  def __init__(self, indexes, timerange, config):
    request_data = {
      "size": 1000,
      "fields": ["transactionId"],
      "query": {
        "filtered": {
          "filter": {
            "bool": {
              "must": [
                { "term":  { "level": "error" } },
                { "range": { "@timestamp" : { "from": totimestamp(timerange[0]), "to": totimestamp(timerange[1]) } } }
              ]
            }
          }
        }
      }
    }

    Request.__init__(self, indexes, request_data, config)

  def parse(self, hits):
    logger.debug("Received %s results", len(hits))
    return [tx['fields']['transactionId'][0] for tx in hits]

def validate_program():
  import doctest
  failures, ran = doctest.testmod()

  if failures > 0:
    sys.exit(-1)

def read_config():
  with open("config.json") as config_file:
    config = json.loads(config_file.read())

  for key in ['username', 'password', 'environment', 'es_url']:
    if not key in config:
      raise NameError(key)
  
  return config

if __name__ == '__main__':
  validate_program()
  config = read_config()

  try:
    run(config)
  except KeyboardInterrupt:
    pass

