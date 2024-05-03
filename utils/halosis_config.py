import configparser
import requests

config = configparser.ConfigParser()
config.read('router.conf')
whatsapp_provider = config.get('satusehat', 'whatsapp_provider')
halosis_email = config.get('satusehat', 'halosis_email')
halosis_pass = config.get('satusehat', 'halosis_pass')

def get_token():
  payload = {
    'email': halosis_email,
    'password': halosis_pass
  }

  response = requests.post(whatsapp_provider+'/v1/login', json=payload)

  if response.status_code == 200:
    response_data = response.json()
    payload = {
      'refresh_token': response_data.get('refresh_token')
    }
    response = requests.post(whatsapp_provider+'/v1/access-token', json=payload)

    if response.status_code == 200:
      response_data = response.json()
      return {
        "token": response_data.get('long_lived_token'),
        "expire_at": response_data.get('token_expired_at'),
      }
    else:
      print("error: ", response.status_code)
  else:
    print("error: ", response.status_code)