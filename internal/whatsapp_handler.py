import requests
from utils import halosis_config
from utils import config
from utils.mongodb import connect_mongodb

config.init()

def send():
  bearer_token = get_valid_token()
  token_expired = check_token_expiry(bearer_token)
  if token_expired:
    all_token_set_false()
    bearer_token = get_valid_token()

  return send_to_whatsapp(bearer_token)

def all_token_set_false():
  db = connect_mongodb()
  collection = db['whatsapp_token']
  filter = {'is_active': True}
  update = {'$set': {'is_active': False}}
  collection.update_many(filter, update)

def get_valid_token():
  db = connect_mongodb()
  collection = db['whatsapp_token']
  exist_token = collection.find_one({"is_active": True})

  if exist_token:
    bearer_token = exist_token["token"]
    return bearer_token
  else:
    bearer_token = halosis_config.get_token()
    newToken = {
      "token": bearer_token["token"],
      "expire_at": bearer_token["expire_at"],
      "is_active": True
    }
    collection.insert_one(newToken)
    return bearer_token["token"]

def check_token_expiry(bearer_token):
  url = config.whatsapp_provider
  headers = {
    "Authorization": f"Bearer {bearer_token}",
    "Content-Type": "application/json"
  }
  response = requests.get(url+'/v1/balance', headers=headers)
  if response.status_code == 200:
    return False
  return True

def send_to_whatsapp(bearer_token):
  url = config.whatsapp_provider
  headers = {
    "Authorization": f"Bearer {bearer_token}",
    "Content-Type": "application/json"
  }
  payload = {
    "messaging_product": "whatsapp",
    "recipient_type": "individual",
    "to": "6281238009823",
    "type": "template",
    "template": {
        "name": "template_halosis",
        "language": {
            "code": "id"
        },
        "components": [
            {
                "type": "header",
                "parameters": [
                    {
                        "type": "text",
                        "text": "Heryanto"
                    }
                ]
            }
        ]
    }
  }

  response = requests.post(url+'/v1/messages', json=payload, headers=headers)

  if response.status_code == 200:
    print("whatsapp message has been sent!")
  else:
    print("error: ", response.json())