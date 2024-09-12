import logging
import os
import shutil
import threading
import pyinotify
from time import sleep
from dotenv import load_dotenv
load_dotenv()

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
from utils.dbquery import DBQuery
from utils import config


def main_loop():
    """
    Main script logic encapsulated in a function.
    This function initializes logging, database, DICOM handlers, and servers.
    """

    # ====================================================
    # Initialization
    # ====================================================
    def initialize_logger():
        """Initialize logger configuration."""
        logging.basicConfig(filename='dicom_router_inotify.log', encoding='utf-8',
                            format='[%(asctime)s] %(message)s', level=logging.INFO)
        LOGGER = logging.getLogger('pynetdicom')
        return LOGGER

    LOGGER = initialize_logger()
    LOGGER.info("[Init] - Starting services")

    # Initialize configuration
    config.init()

    # Setup database
    dbq = DBQuery()

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


def setup_env():
    """
    Setup the environment variables.
    """
    loop = True

    while loop:
        # Environment variables
        url = os.getenv('URL', 'Not Set')
        organization_id = os.getenv('ORGANIZATION_ID', 'Not Set')
        dicom_pathsuffix = os.getenv('DICOM_PATHSUFFIX', 'Not Set')
        fhir_pathsuffix = os.getenv('FHIR_PATHSUFFIX', 'Not Set')
        self_ae_title = os.getenv('AE_TITLE', 'Not Set')

        dicom_port = os.getenv('DICOM_PORT', '11112')  # Default to 11112 if not set
        dcm_dir = os.getenv('DCM_DIR', 'Not Set')
        http_port = os.getenv('HTTP_PORT', '8083')  # Default to 8083 if not set
        flask_port = os.getenv('FLASK_PORT', '8082')  # Default to 8082 if not set
        inotify_dir = os.getenv('INOTIFY_DIR', 'Not Set')

        mongodb_url = os.getenv('MONGODB_URL', 'Not Set')
        pacs_db_name = os.getenv('PACS_DB_NAME', 'Not Set')
        whatsapp_provider = os.getenv('WHATSAPP_PROVIDER', 'Not Set')
        mroc_client_url = os.getenv('MROC_CLIENT_URL', 'Not Set')

        client_key = os.getenv('CLIENT_KEY', 'Not Set')
        secret_key = os.getenv('SECRET_KEY', 'Not Set')

        # Display the current values
        print(f"1. URL: {url}")
        print(f"2. ORGANIZATION_ID: {organization_id}")
        print(f"3. DICOM_PATHSUFFIX: {dicom_pathsuffix}")
        print(f"4. FHIR_PATHSUFFIX: {fhir_pathsuffix}")
        print(f"5. AE_TITLE: {self_ae_title}")
        print(f"6. DICOM_PORT: {dicom_port}")
        print(f"7. DCM_DIR: {dcm_dir}")
        print(f"8. HTTP_PORT: {http_port}")
        print(f"9. FLASK_PORT: {flask_port}")
        print(f"10. INOTIFY_DIR: {inotify_dir}")
        print(f"11. MONGODB_URL: {mongodb_url}")
        print(f"12. PACS_DB_NAME: {pacs_db_name}")
        print(f"13. WHATSAPP_PROVIDER: {whatsapp_provider}")
        print(f"14. MROC_CLIENT_URL: {mroc_client_url}")
        print(f"15. CLIENT_KEY: {client_key}")
        print(f"16. SECRET_KEY: {secret_key}")
        print("0. Exit")

        index = input("Select an option to change (or '0' to exit): ")

        if index == "1":
            os.environ['URL'] = input("Enter new value for URL: ")
        elif index == "2":
            os.environ['ORGANIZATION_ID'] = input("Enter new value for ORGANIZATION_ID: ")
        elif index == "3":
            os.environ['DICOM_PATHSUFFIX'] = input("Enter new value for DICOM_PATHSUFFIX: ")
        elif index == "4":
            os.environ['FHIR_PATHSUFFIX'] = input("Enter new value for FHIR_PATHSUFFIX: ")
        elif index == "5":
            os.environ['AE_TITLE'] = input("Enter new value for AE_TITLE: ")
        elif index == "6":
            os.environ['DICOM_PORT'] = input("Enter new value for DICOM_PORT: ")
        elif index == "7":
            os.environ['DCM_DIR'] = input("Enter new value for DCM_DIR: ")
        elif index == "8":
            os.environ['HTTP_PORT'] = input("Enter new value for HTTP_PORT: ")
        elif index == "9":
            os.environ['FLASK_PORT'] = input("Enter new value for FLASK_PORT: ")
        elif index == "10":
            os.environ['INOTIFY_DIR'] = input("Enter new value for INOTIFY_DIR: ")
        elif index == "11":
            os.environ['MONGODB_URL'] = input("Enter new value for MONGODB_URL: ")
        elif index == "12":
            os.environ['PACS_DB_NAME'] = input("Enter new value for PACS_DB_NAME: ")
        elif index == "13":
            os.environ['WHATSAPP_PROVIDER'] = input("Enter new value for WHATSAPP_PROVIDER: ")
        elif index == "14":
            os.environ['MROC_CLIENT_URL'] = input("Enter new value for MROC_CLIENT_URL: ")
        elif index == "15":
            os.environ['CLIENT_KEY'] = input("Enter new value for CLIENT_KEY: ")
        elif index == "16":
            os.environ['SECRET_KEY'] = input("Enter new value for SECRET_KEY: ")
        elif index == "0":
            loop = False
        else:
            print("Invalid option. Please select a valid option.")


def setup():
    """
    Set up the script environment.
    """
    loop = True
    while loop:
        print("1. Start")
        print("2. Setup")
        print("0. Exit")
        index = input("Select an option: ")

        if index == "1":
            main_loop()  # Replace with your main script logic
        elif index == "2":
            setup_env()
        elif index == "0":
            loop = False
        else:
            print("Invalid option")


if __name__ == "__main__":
    print("[SYSTEM] Starting...")
    setup()
    print("[SYSTEM] Shutting down...")
