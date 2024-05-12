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
    global token
    payload = {
        'client_id': client_key,
        'client_secret': secret_key
    }
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        # 'User-Agent': 'curl/7.64.1',
        'User-Agent': 'PostmanRuntime/7.26.8',
    }
    try:
        res = requests.post(url+"/oauth2/v1/accesstoken?grant_type=client_credentials", data=payload, headers=headers, verify=False)
        # Print response headers
        print("OAuth2 Response headers:", res.headers)
        
        # Print request headers after making the request
        print("OAuth2 Request headers:", res.request.headers)

        print("res: client=" + client_key + ";secret=" + secret_key + ";url=" + url + "/oauth2/v1/accesstoken?grant_type=client_credentials" + ";payload=" + str(payload) + ";headers=" + str(headers) + ";res=" + str(res))
        res.raise_for_status()  # Raise an exception for HTTP errors (status codes 4xx and 5xx)
        
        
        data = res.json()


        print("OAuth2 Response data:", data)

        token = data["access_token"]
        print("OAuth2 token:", token)
        return token
    except requests.exceptions.RequestException as e:
        print("Request failed:", e)
        return None
