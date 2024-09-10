import configparser
import logging
import os
import shutil
import threading
import pyinotify
from time import sleep

from flask import Flask
from pynetdicom import AE, evt, AllStoragePresentationContexts, debug_logger, StoragePresentationContexts, ALL_TRANSFER_SYNTAXES
from pynetdicom.sop_class import (
    Verification,
    PatientRootQueryRetrieveInformationModelFind,
    PatientRootQueryRetrieveInformationModelMove,
    PatientRootQueryRetrieveInformationModelGet,
    StudyRootQueryRetrieveInformationModelFind,
    StudyRootQueryRetrieveInformationModelMove,
    StudyRootQueryRetrieveInformationModelGet,
    ModalityWorklistInformationFind,
)

from internal import dicom_listener, dicom_handler, http_server, whatsapp_handler
from internal.flask_server import app
from utils.dicom2fhir import process_dicom_2_fhir
from utils.dbquery import dbquery
from utils import config

# ====================================================
# Initialization
# ====================================================
def initialize_logger():
    """Initialize logger configuration."""
    debug_logger()
    LOGGER = logging.getLogger('pynetdicom')
    FORMAT = '[%(asctime)s] %(message)s'
    logging.basicConfig(filename='dicom_router_inotify.log', encoding='utf-8', format=FORMAT, level=logging.INFO)
    return LOGGER

LOGGER = initialize_logger()
LOGGER.info("[Init] - Starting services")

# Initialize configuration
config.init()

# Setup database
dbq = dbquery()

# ====================================================
# Event Handlers Setup
# ====================================================
LOGGER.info("[Init] - Setting up DICOM handlers")

handlers = [
    (evt.EVT_C_STORE, dicom_handler.handle_store, [config.dcm_dir, LOGGER]),
    (evt.EVT_RELEASED, dicom_handler.handle_assoc_released, [config.dcm_dir, config.organization_id, config.mroc_client_url, config.encrypt, LOGGER]),
    (evt.EVT_C_ECHO, dicom_handler.handle_echo, [LOGGER]),
    (evt.EVT_C_FIND, dicom_handler.handle_find, [LOGGER]),
]

# ====================================================
# Application Entity (AE) Setup
# ====================================================
LOGGER.info("[Init] - Initializing Application Entity (AE)")
ae = AE(ae_title=config.self_ae_title)

# Add supported presentation contexts
transfer_syntaxes = ALL_TRANSFER_SYNTAXES
for context in StoragePresentationContexts:
    ae.add_supported_context(context.abstract_syntax, transfer_syntaxes)

# Support verification SCP (echo) and query/retrieve SCPs
ae.add_supported_context(Verification)
ae.add_supported_context(PatientRootQueryRetrieveInformationModelFind)
ae.add_supported_context(PatientRootQueryRetrieveInformationModelMove)
ae.add_supported_context(PatientRootQueryRetrieveInformationModelGet)
ae.add_supported_context(StudyRootQueryRetrieveInformationModelFind)
ae.add_supported_context(StudyRootQueryRetrieveInformationModelMove)
ae.add_supported_context(StudyRootQueryRetrieveInformationModelGet)
ae.add_supported_context(ModalityWorklistInformationFind)

# Support all storage SOP Classes
ae.supported_contexts = AllStoragePresentationContexts

# Require Called AE Title to match
ae.require_called_aet = config.self_ae_title

# ====================================================
# Folder Cleanup and Initialization
# ====================================================
LOGGER.info("[Init] - Clearing and preparing incoming folder")

incoming_dir = os.path.join(os.getcwd(), config.dcm_dir)
try:
    shutil.rmtree(incoming_dir)
except Exception as err:
    LOGGER.error(f"Error while clearing folder: {err}")

os.makedirs(incoming_dir, exist_ok=True)

# ====================================================
# Inotify Event Handler
# ====================================================
class EventHandler(pyinotify.ProcessEvent):
    """Handles the creation of files in the watched directory."""
    def process_IN_CREATE(self, event):
        # Push DICOM file on creation
        dicom_listener.dicom_push(event.pathname)

# ====================================================
# Flask Server Thread
# ====================================================
def flask_server():
    """Starts the Flask server."""
    LOGGER.info(f'[Init] - Starting Flask service on port {config.flask_port}')
    if __name__ == '__main__':
        app.run(host="0.0.0.0", port=config.flask_port)

flask_thread = threading.Thread(target=flask_server)
flask_thread.start()

# ====================================================
# DICOM Server Initialization
# ====================================================
pid = os.fork()

if pid > 0:
    # Parent process: start DICOM interface
    LOGGER.info(f"[Init] - Spawning DICOM interface on port {config.dicom_port} with AE title: {config.self_ae_title}.")
    ae.start_server(("0.0.0.0", config.dicom_port), evt_handlers=handlers)
else:
    # Child process: start HTTP server
    LOGGER.info(f'[Init] - Starting HTTP service on port {config.http_port}...')
    http_server.start_server(config.http_port)
