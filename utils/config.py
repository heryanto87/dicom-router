import os
from utils import oauth2
from interface import satusehat

from dotenv import load_dotenv
load_dotenv()

def init():
    global url, organization_id, dicom_pathsuffix, fhir_pathsuffix, dicom_port, dcm_dir, http_port, self_ae_title, mroc_client_url, encrypt
    global client_key, secret_key, token, dcm_config
    global flask_port, inotify_dir, mongodb_url, whatsapp_provider, pacs_db_name

    # SATUSEHAT Configuration (loaded from environment variables)
    url = os.getenv('URL')
    organization_id = os.getenv('ORGANIZATION_ID')
    dicom_pathsuffix = os.getenv('DICOM_PATHSUFFIX')
    fhir_pathsuffix = os.getenv('FHIR_PATHSUFFIX')
    self_ae_title = os.getenv('AE_TITLE')

    # Ports and Directories
    dicom_port = int(os.getenv('DICOM_PORT', 11112))  # Default to 11112 if not set
    dcm_dir = os.getenv('DCM_DIR')
    http_port = int(os.getenv('HTTP_PORT', 8083))  # Default to 8083 if not set
    flask_port = int(os.getenv('FLASK_PORT', 8082))  # Default to 8082 if not set
    inotify_dir = os.getenv('INOTIFY_DIR')

    # Database and External URLs
    mongodb_url = os.getenv('MONGODB_URL')
    pacs_db_name = os.getenv('PACS_DB_NAME')
    whatsapp_provider = os.getenv('WHATSAPP_PROVIDER')
    mroc_client_url = os.getenv('MROC_CLIENT_URL')

    # OAuth credentials
    client_key = os.getenv('CLIENT_KEY')
    secret_key = os.getenv('SECRET_KEY')

    # Get token using OAuth2
    token = oauth2.get_token()

    # Get DICOM configuration
    dcm_config = satusehat.get_dcm_config(token)

    # Enable encryption based on the environment variable, default to False if not found or set
    encrypt = os.getenv('ENCRYPT', 'false').lower() == 'true'
