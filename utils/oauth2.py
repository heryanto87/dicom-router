import logging
import requests
import configparser

LOGGER = logging.getLogger('pynetdicom')

config = configparser.ConfigParser()
config.read('router.conf')
url = config.get('satusehat', 'url')
client_key = config.get('satusehat', 'client_key')
secret_key = config.get('satusehat', 'secret_key')

def get_token():

  # global token
  # payload = 'client_id='+client_key+'&client_secret='+secret_key
  # headers = {
  #   'Content-Type': 'application/x-www-form-urlencoded'
  # }
  # try:
  #   res = requests.post(url+"/oauth2/v1/accesstoken?grant_type=client_credentials", data=payload, headers=headers)
  #   # data = res.json()
  #   data = res if res.status_code != 200 else res.json()
  # except Exception as e: # work on python 3.x
  #   LOGGER.exception(e)
  #   LOGGER.error("Authentication failed")
  #   return ""
  # token = data["access_token"]
  # return data["access_token"]

  return "fake_token"

