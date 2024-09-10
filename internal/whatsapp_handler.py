import requests
import logging
from utils import halosis_config, config
from utils.mongodb import connect_mongodb, client_mongodb

# Initialize configurations and logger
config.init()
LOGGER = logging.getLogger("flask_server")

# MongoDB client and database connection
_client = client_mongodb()
_db = _client[config.pacs_db_name]

def send(patientPhoneNumber, previewImage, patientName, examination, hospital, date, link):
    """Send a WhatsApp message with patient examination details."""
    bearer_token = get_valid_token()

    if check_token_expiry(bearer_token):
        deactivate_all_tokens()
        bearer_token = get_valid_token()

    return send_to_whatsapp(
        bearer_token, patientPhoneNumber, previewImage,
        patientName, examination, hospital, date, link
    )

def deactivate_all_tokens():
    """Set all active tokens to inactive in the database."""
    collection = _db['whatsapp_token']
    collection.update_many({'is_active': True}, {'$set': {'is_active': False}})

def get_valid_token():
    """Retrieve a valid bearer token, or generate a new one if necessary."""
    collection = _db['whatsapp_token']
    active_token = collection.find_one({"is_active": True})

    if active_token:
        return active_token["token"]

    new_token_data = halosis_config.get_token()
    new_token = {
        "token": new_token_data["token"],
        "expire_at": new_token_data["expire_at"],
        "is_active": True
    }
    collection.insert_one(new_token)
    return new_token_data["token"]

def check_token_expiry(bearer_token):
    """Check if the current token has expired."""
    url = config.whatsapp_provider
    headers = {
        "Authorization": f"Bearer {bearer_token}",
        "Content-Type": "application/json",
        'User-Agent': 'PostmanRuntime/7.26.8',
    }

    response = requests.get(f"{url}/v1/balance", headers=headers)
    return response.status_code != 200

def send_to_whatsapp(bearer_token, patientPhoneNumber, previewImage, patientName, examination, hospital, date, link):
    """Send a WhatsApp message using the provided token and message details."""
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
            "language": {"code": "en_US"},
            "components": [
                {
                    "type": "header",
                    "parameters": [{"type": "image", "image": {"link": previewImage}}]
                },
                {
                    "type": "body",
                    "parameters": [
                        {"type": "text", "text": patientName},
                        {"type": "text", "text": f"*{examination}*"},
                        {"type": "text", "text": hospital},
                        {"type": "text", "text": date},
                        {"type": "text", "text": link},
                        {"type": "text", "text": "*Perhatian*: _Link ini akan kadaluarsa setelah 30 hari_"}
                    ]
                }
            ]
        }
    }

    response = requests.post(f"{url}/v1/messages", json=payload, headers=headers)

    if response.status_code == 200:
        LOGGER.info("WhatsApp message has been sent!")
    else:
        LOGGER.error(f"Error sending WhatsApp message: {response.json()}")
