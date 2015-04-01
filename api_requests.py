import time, logging
import requests
import json

from requests.auth import HTTPBasicAuth

#Setup logging
logger = logging.getLogger(__name__)

def totimestamp(dt):
  """Converts datetime to timestamp"""

  return int(time.mktime(dt.timetuple())) * 1000 

def shards_failed(response):
  return response['_shards']['failed'] > 0

class RequestUnsuccesfulError(Exception):
  def __init__(self, status_code):
    self.status_code = status_code

  def __str__(self):
    return repr(self.status_code)

class Request(object):
  """
  Base request to the elastic search instance search API
  """
  def __init__(self, indexes, request_data, config, logger=None):
    self.indexes = indexes
    self.request_data = request_data
    self.config = config
    self.environment = self.config_value('environment')
    self.logger = logger or logging.getLogger(__name__)


  def run(self):
    response = requests.post("%s%s/_search" % (self.config['es_url'], ",".join(self.indexes)), 
                            auth=HTTPBasicAuth(self.config['username'], self.config['password']), 
                            data=json.dumps(self.request_data))
    
    self.logger.debug("Launching request")
    if response.status_code == 200:
      json_response = response.json()
      if shards_failed(json_response):
        self.logger.error("Some shards failed to respond")

      page_result = json_response['hits']
      
      return self.parse(page_result['hits'])
    else:
      raise RequestUnsuccesfulError(response.status_code)

  def read_field(self, entry, fieldname):
    if fieldname in entry['fields']:
      return entry['fields'][fieldname][0]

    return None

  def read_fields(self, entry, fieldnames):
    return tuple([self.read_field(entry, field) for field in fieldnames])

  def parse(self, hits):
    return hits

  def config_value(self, key):
    if key in self.config:
      return self.config[key]

    return None

class ErrorTransactionListing(Request):
  """
  Fetches the API transaction which ended in error
  """
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
    self.logger.debug("Received %s results", len(hits))
    return [tx['fields']['transactionId'][0] for tx in hits]

class TransactionDetail(Request):
  """
  Fetches the detail of an API transaction
  """
  def __init__(self, indexes, transactionId, config):
    self.transactionId = transactionId

    request_data = {
       "sort": "@timestamp",
       "fields": ["@timestamp", "loggerName", "message", "stacktrace", "transactionId", "proteus-username", "referer"],
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

  def log_to_slack(self, message):
    slack_conf = self.config_value('slack_bot')

    requests.get("https://slack.com/api/chat.postMessage?token=%s&channel=%s&text=%s" % (slack_conf['token'], slack_conf['channel'], message))

  def parse(self, hits):
    result = []
    self.logger.debug("Fetching transaction detail for %s", self.transactionId)
    username = None
    referer = None
    for entry in hits:
      tstamp, tx, className, msg, stack = self.read_fields(entry, ('@timestamp', 'transactionId', 'loggerName', 'message', 'stacktrace'))
      raw_user, raw_referer = self.read_fields(entry, ('proteus-username', 'referer'))

      result.append({
        'transaction': tx,
        'time': tstamp,
        'class':  className,
        'message': msg,
        'stack': stack,
      })

      if raw_user:
        username = raw_user

      if raw_referer:
        referer = raw_referer

    for parsed in result:
      parsed['username'] = username
      parsed['referer'] = referer

    error_message = result[-1]['message'][:50]
    message = "[%s] %s -> %s (%s)" % (self.environment, self.transactionId, error_message, username)

    if self.config_value('slack_bot'):
      self.log_to_slack(message)
      

    self.logger.info(message)

    return result

