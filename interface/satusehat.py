import configparser
import logging
import os
import requests

from utils.dbquery import DBQuery
from utils import oauth2, halosis_config
from utils.dicomutils import make_hash

# Initialize configuration
config = configparser.ConfigParser()
config.read("router.conf")

# Setup logging
LOGGER = logging.getLogger("pynetdicom")

# Database query instance
dbq = DBQuery()

# Configuration variables
url = config.get("satusehat", "url")
dicom_pathsuffix = config.get("satusehat", "dicom_pathsuffix")
fhir_pathsuffix = config.get("satusehat", "fhir_pathsuffix")
organization_id = config.get("satusehat", "organization_id")
dcm_dir = config.get("satusehat", "dcm_dir")


def send(patientPhoneNumber, previewImage, patientName, examination, hospital, date, link):
    """Send a WhatsApp message."""
    bearer_token = get_valid_token()
    if check_token_expiry(bearer_token):
        deactivate_all_tokens()
        bearer_token = get_valid_token()

    return send_to_whatsapp(bearer_token, patientPhoneNumber, previewImage, patientName, examination, hospital, date, link)


def deactivate_all_tokens():
    """Deactivate all active tokens in the database."""
    collection = dbq._db['whatsapp_token']
    collection.update_many({'is_active': True}, {'$set': {'is_active': False}})


def get_valid_token():
    """Retrieve a valid bearer token, or generate a new one if necessary."""
    collection = dbq._db['whatsapp_token']
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
    headers = {
        "Authorization": f"Bearer {bearer_token}",
        "Content-Type": "application/json",
        'User-Agent': 'PostmanRuntime/7.26.8',
    }
    response = requests.get(f"{url}/v1/balance", headers=headers)
    return response.status_code != 200


def send_to_whatsapp(bearer_token, patientPhoneNumber, previewImage, patientName, examination, hospital, date, link):
    """Send a WhatsApp message using the provided token and message details."""
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


def get_service_request(accessionNumber):
    """Retrieve a ServiceRequest based on the accession number."""
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {oauth2.token}",
        'User-Agent': 'PostmanRuntime/7.26.8',
    }
    path = (f"{fhir_pathsuffix}/ServiceRequest?identifier=http://sys-ids.kemkes.go.id/acsn/"
            f"{organization_id}%7C{accessionNumber}&_sort=-_lastUpdated&_count=1")

    response = requests.get(url=f"{url}{path}", headers=headers)
    data = response.json()

    if data.get("resourceType") == "Bundle" and data.get("total", 0) >= 1:
        _, patientID = data["entry"][0]["resource"]["subject"]["reference"].split("/")
        return data["entry"][0]["resource"]["id"], patientID

    raise Exception("ServiceRequest not found")


def get_imaging_study(accessionNumber, token):
    """Retrieve an ImagingStudy based on the accession number."""
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {token}",
        'User-Agent': 'PostmanRuntime/7.26.8',
    }
    path = (f"{fhir_pathsuffix}/ImagingStudy?identifier=http://sys-ids.kemkes.go.id/acsn/"
            f"{organization_id}%7C{accessionNumber}&_sort=-_lastUpdated&_count=1")

    response = requests.get(url=f"{url}{path}", headers=headers)
    data = response.json()

    if data.get("resourceType") == "Bundle" and data.get("total", 0) >= 1:
        _, patientID = data["entry"][0]["resource"]["subject"]["reference"].split("/")
        return data["entry"][0]["resource"]["id"]

    return None


def imagingstudy_post(filename, id):
    """Post or update an ImagingStudy."""
    headers = {
        "Authorization": f"Bearer {oauth2.token}",
        "Content-Type": "application/json",
        'User-Agent': 'PostmanRuntime/7.26.8',
    }

    with open(filename, "rb") as payload:
        if id is None:
            response = requests.post(url=f"{url}{fhir_pathsuffix}/ImagingStudy", data=payload, headers=headers)
        else:
            response = requests.put(url=f"{url}{fhir_pathsuffix}/ImagingStudy/{id}", data=payload, headers=headers)

    data = response.json()
    LOGGER.info(data)

    if data.get("resourceType") == "ImagingStudy":
        return data["id"]

    raise Exception("POST ImagingStudy failed")


def dicom_push(assocId, study_iuid, imagingStudyID):
    """Push DICOM instances to the server."""
    if imagingStudyID is None:
        return None

    LOGGER.info("DICOM Push started")
    subdir = make_hash(assocId)
    LOGGER.info(f"DICOM Push ImagingStudyID: {imagingStudyID}")

    headers = {
        "Content-Type": "application/dicom",
        "Accept": "application/dicom+json",
        "Authorization": f"Bearer {oauth2.token}",
        "X-ImagingStudy-ID": imagingStudyID,
        'User-Agent': 'PostmanRuntime/7.26.8',
    }

    instances = dbq.Query(dbq.GET_INSTANCES_PER_STUDY, [assocId, study_iuid])

    for series_iuid, instance_uid in instances:
        filename = os.path.join(os.getcwd(), dcm_dir, subdir, study_iuid, series_iuid, f"{instance_uid}.dcm")
        try:
            with open(filename, "rb") as payload:
                response = requests.post(url=f"{url}{dicom_pathsuffix}", data=payload, headers=headers)

            if response.status_code == 200:
                LOGGER.info(f"Sending Instance UID: {series_iuid}/{instance_uid} success")
                dbq.Update(dbq.UPDATE_INSTANCE_STATUS_SENT, [assocId, study_iuid, series_iuid, instance_uid])
            else:
                LOGGER.error(f"Error sending Instance UID {instance_uid}: {response.json()}")
                if "Instance already exists" in response.text:
                    LOGGER.warning("Image already exists")
                    os.remove(filename)  # Remove the DICOM file if it already exists
                    # Remove Series UID Folder if Empty
                    os.rmdir(os.path.dirname(filename))
        except Exception as e:
            LOGGER.error(f"Sending DICOM failed: {e}")
            raise Exception("Sending DICOM failed")

    return True


def get_dcm_config(token):
    """Retrieve DICOM configuration."""
    headers = {
        "Accept": "application/json",
        "User-Agent": "PostmanRuntime/7.26.8",
        "Authorization": f"Bearer {token}"
    }
    path = f"{fhir_pathsuffix}/dcm_cfg"
    response = requests.get(url=f"{url}{path}", headers=headers)
    return response.json()
