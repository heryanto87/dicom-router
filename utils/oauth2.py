import logging
import requests
from utils import config

LOGGER = logging.getLogger('pynetdicom')

def get_token():
  global token
  payload = 'client_id='+config.client_key+'&client_secret='+config.secret_key
  headers = {
    'Content-Type': 'application/x-www-form-urlencoded'
  }
  try:
    res = requests.post(url=config.url+"/oauth2/v1/accesstoken?grant_type=client_credentials", data=payload, headers=headers)
    data = res.json()
  except Exception as e: # work on python 3.x
    LOGGER.exception(e)
    LOGGER.error("Authentication failed")
    return ""
  token = data["access_token"]
  return data["access_token"]

