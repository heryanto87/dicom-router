import configparser
from utils import oauth2
from interface import satusehat

def init():
  global config, url, organization_id, dicom_pathsuffix, fhir_pathsuffix, dicom_port, dcm_dir, http_port, self_ae_title, mroc_client_url, encrypt
  global client_key, secret_key
  global token, dcm_config
  global flask_port, inotify_dir, mongodb_url, whatsapp_provider, pacs_db_name

  config = configparser.ConfigParser()
  config.read('router.conf')
  url = config.get('satusehat', 'url')
  organization_id = config.get('satusehat', 'organization_id')
  dicom_pathsuffix = config.get('satusehat', 'dicom_pathsuffix')
  fhir_pathsuffix = config.get('satusehat', 'fhir_pathsuffix')
  self_ae_title = config.get('satusehat', 'ae_title')
  dicom_port = int(config.get('satusehat', 'dicom_port'))
  dcm_dir = config.get('satusehat', 'dcm_dir')
  http_port = int(config.get('satusehat', 'http_port'))
  client_key = config.get('satusehat', 'client_key')
  secret_key = config.get('satusehat', 'secret_key')
  mroc_client_url = config.get('satusehat', 'mroc_client_url')
  token = oauth2.get_token()
  dcm_config = satusehat.get_dcm_config(token)

  flask_port = config.get('satusehat', 'flask_port')
  inotify_dir = config.get('satusehat', 'inotify_dir')
  mongodb_url = config.get('satusehat', 'mongodb_url')
  whatsapp_provider = config.get('satusehat', 'whatsapp_provider')
  pacs_db_name = config.get('satusehat', 'pacs_db_name')

  try:
    encrypt = dcm_config['SATUSEHAT_CLIENT_ENABLE']
  except:
    encrypt = False


