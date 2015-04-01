import websocket, requests
import thread
import time
import json

token = ""

users = {}
channels = {}
groups = {}

message_id = 0

class SlackErrorBot(websocket.WebSocketApp):
  """docstring for SlackErrorBot"""

  triggers = {
    'cat': 'cat_file'
  }

  def __init__(self, url):
    super(SlackErrorBot, self).__init__(url, 
                      on_message = self.on_message,
                      on_error = self.on_error,
                      on_close = self.on_close)
    self.m_id = 0

  def cat_file(self, msg):
    self.send_message(msg['channel'], json.dumps(msg))

  def next_id(self):
    self.m_id = self.m_id + 1
    return self.m_id

  def send_message(self, channel, text):
    self.send(json.dumps({
      'channel': channel,
      'type': 'message',
      'id': self.next_id(),
      'text': text
    }))

  def on_open(self):
    print 'open'

  def on_message(self, ws, raw_msg):
    msg = json.loads(raw_msg)

    if 'type' in msg and msg['type'] == 'message':
      channel = msg['channel']
      text = msg['text']

      if text in self.triggers:
        getattr(self, self.triggers[text])(msg)

      if channel in channels:
        channel = channels[channel]['name']

      if channel in groups:
        channel = groups[channel]['name']

      if 'user' in msg:
        user = msg['user']
      elif 'bot_id' in msg:
        user = msg['bot_id']

      if user in users:
        user = users[user]['name']

      print "[(%s)%s] %s" % (channel, user, text)
    else:
      print msg

  def on_error(ws, error):
    print error

  def on_close(ws):
    print "Closed."


def index(items):
  res = {}
  for item in items:
    res[item['id']] = item
  return res

if __name__ == '__main__':
  websocket.enableTrace(True)
  ws_response = requests.get("https://slack.com/api/rtm.start?token=%s" % (token, ))
  json_response = ws_response.json()

  if ws_response.status_code == 200 and json_response['ok']:
    users = index(json_response['users'])
    channels = index(json_response['channels'])
    groups = index(json_response['groups'])

    ws = SlackErrorBot(json_response['url'])
    ws.run_forever()
  else:
    print "Error!!"
    print ws_response
