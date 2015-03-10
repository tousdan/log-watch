import json
import os
import logging

logger = logging.getLogger(__name__)

def read_config(config_file):
  config = json.loads(config_file.read())

  for key in ['username', 'password', 'environment', 'es_url', 'logs_dir']:
    if not key in config:
      raise NameError(key)
  
  return config

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

def get_transaction(tx_dir, date_folder, transactionId):
  path = os.path.join(tx_dir, date_folder, transactionId)

  if has_transaction(tx_dir, date_folder, transactionId):
    with open(path, "r") as in_data:
      return in_data.read()

  return None