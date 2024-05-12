import requests
from utils import halosis_config
from utils import config
from utils.mongodb import connect_mongodb, client_mongodb
import logging

config.init()

LOGGER = logging.getLogger("flask_server")

_client = client_mongodb()
_db = _client[config.pacs_db_name]

def send(
  patientPhoneNumber, 
  previewImage, 
  patientName, 
  examination, 
  hospital, 
  date, 
  link
):
  bearer_token = get_valid_token()
  token_expired = check_token_expiry(bearer_token)
  if token_expired:
    all_token_set_false()
    bearer_token = get_valid_token()

  return send_to_whatsapp(
    bearer_token,
    patientPhoneNumber, 
    previewImage, 
    patientName, 
    examination, 
    hospital, 
    date, 
    link
  )

def all_token_set_false():
  collection = _db['whatsapp_token']
  filter = {'is_active': True}
  update = {'$set': {'is_active': False}}
  collection.update_many(filter, update)

def get_valid_token():
  collection = _db['whatsapp_token']
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
    "Content-Type": "application/json",
    'User-Agent': 'PostmanRuntime/7.26.8',
  }
  response = requests.get(url+'/v1/balance', headers=headers)
  if response.status_code == 200:
    return False
  return True

def send_to_whatsapp(
  bearer_token, 
  patientPhoneNumber, 
  previewImage, 
  patientName, 
  examination, 
  hospital, 
  date, 
  link
):
  url = config.whatsapp_provider
  headers = {
    "Authorization": f"Bearer {bearer_token}",
    "Content-Type": "application/json",
    'User-Agent': 'PostmanRuntime/7.26.8',
  }
  payload = {
    "messaging_product": "whatsapp",
    "recipient_type": "individual",
    "to": patientPhoneNumber,
    "type": "template",
    "template": {
        "name": "pemeriksaan_pasien_3",
        "language": {
            "code": "en_US"
        },
        "components": [
            {
                "type": "header",
                "parameters": [
                    {
                        "type": "image",
                        "image": {
                            "link": previewImage
                        }
                    }
                ]
            }, {
                "type": "body",
                "parameters": [
                    {
                        "type": "text",
                        "text": patientName
                    },
                    {
                        "type": "text",
                        "text": "*" + examination + "*"
                    },
                    {
                        "type": "text",
                        "text": hospital
                    },
                    {
                        "type": "text",
                        "text": date
                    },
                    {
                        "type": "text",
                        "text": link
                    },
                    {
                        "type": "text",
                        "text": "*Perhatian*: _Link ini akan kadaluarsa setelah 30 hari_"
                    },
                ]
            }
        ]
    }
  }

  response = requests.post(url+'/v1/messages', json=payload, headers=headers)

  if response.status_code == 200:
    LOGGER.info("whatsapp message has been sent!")
  else:
    LOGGER.info("error: ", response.json())