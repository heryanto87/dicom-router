import configparser
from utils import oauth2
from interface import satusehat

def init():
    global config, url, organization_id, dicom_pathsuffix, fhir_pathsuffix, dicom_port, dcm_dir, http_port, self_ae_title, mroc_client_url, encrypt
    global client_key, secret_key, token, dcm_config
    global flask_port, inotify_dir, mongodb_url, whatsapp_provider, pacs_db_name

    # Load configuration file
    config = configparser.ConfigParser()
    config.read('router.conf')

    # SATUSEHAT Configuration
    url = config.get('satusehat', 'url')
    organization_id = config.get('satusehat', 'organization_id')
    dicom_pathsuffix = config.get('satusehat', 'dicom_pathsuffix')
    fhir_pathsuffix = config.get('satusehat', 'fhir_pathsuffix')
    self_ae_title = config.get('satusehat', 'ae_title')

    # Ports and Directories
    dicom_port = config.getint('satusehat', 'dicom_port')
    dcm_dir = config.get('satusehat', 'dcm_dir')
    http_port = config.getint('satusehat', 'http_port')
    flask_port = config.getint('satusehat', 'flask_port')
    inotify_dir = config.get('satusehat', 'inotify_dir')

    # Database and External URLs
    mongodb_url = config.get('satusehat', 'mongodb_url')
    pacs_db_name = config.get('satusehat', 'pacs_db_name')
    whatsapp_provider = config.get('satusehat', 'whatsapp_provider')
    mroc_client_url = config.get('satusehat', 'mroc_client_url')

    # OAuth credentials
    client_key = config.get('satusehat', 'client_key')
    secret_key = config.get('satusehat', 'secret_key')

    # Get token using OAuth2
    token = oauth2.get_token()

    # Get DICOM configuration
    dcm_config = satusehat.get_dcm_config(token)

    # Enable encryption based on the config, default to False if key not found
    encrypt = dcm_config.get('SATUSEHAT_CLIENT_ENABLE', False)
