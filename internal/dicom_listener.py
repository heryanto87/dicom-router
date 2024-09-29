import time
import zipfile
import os
from pydicom import dcmread
from pynetdicom import AE, AllStoragePresentationContexts, StoragePresentationContexts, ALL_TRANSFER_SYNTAXES
from pynetdicom.presentation import PresentationContext
from pydicom.errors import InvalidDicomError
from utils.mongodb import connect_mongodb, client_mongodb
from pymongo.errors import OperationFailure
from datetime import datetime
from utils import config
import logging
from pynetdicom.sop_class import Verification

from dotenv import load_dotenv
load_dotenv()

# Initialize the configuration
config.init()

# Logger initialization
LOGGER = logging.getLogger("flask_server")

# Allowed file extensions
ALLOWED_EXTENSIONS = {'dcm', 'zip'}

# MongoDB client and database connection
_client = client_mongodb()
_db = _client[config.pacs_db_name]

def allowed_file(filename):
    """Check if the file extension is allowed."""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def generate_metadata(collection, file_dcm, pathname):
    """Generate metadata for the specified DICOM collection."""
    common_metadata = {
        "patient_id": str(file_dcm.PatientID) if hasattr(file_dcm, 'PatientID') else None,
    }

    if collection == "patient":
        return {
            **common_metadata,
            "patient_name": str(file_dcm.PatientName) if hasattr(file_dcm, 'PatientName') else None,
        }
    elif collection == "study":
        return {
            **common_metadata,
            "study_id": str(file_dcm[0x0020, 0x0010].value) if [0x0020, 0x0010] in file_dcm else None,
            "study_instance_uid": str(file_dcm.StudyInstanceUID) if hasattr(file_dcm, 'StudyInstanceUID') else None,
            "study_date": str(file_dcm.StudyDate) if hasattr(file_dcm, 'StudyDate') else None,
            "study_time": str(file_dcm.StudyTime) if hasattr(file_dcm, 'StudyTime') else None,
            "study_description": str(file_dcm.StudyDescription) if hasattr(file_dcm, 'StudyDescription') else None,
            "accession_number": str(file_dcm.AccessionNumber) if hasattr(file_dcm, 'AccessionNumber') else None,
        }
    elif collection == "series":
        return {
            **common_metadata,
            "study_id": str(file_dcm[0x0020, 0x0010].value) if [0x0020, 0x0010] in file_dcm else None,
            "series_number": str(file_dcm.SeriesNumber) if hasattr(file_dcm, 'SeriesNumber') else None,
            "series_instance_uid": str(file_dcm.SeriesInstanceUID) if hasattr(file_dcm, 'SeriesInstanceUID') else None,
            "series_date": str(file_dcm.SeriesDate) if hasattr(file_dcm, 'SeriesDate') else None,
            "series_time": str(file_dcm.SeriesTime) if hasattr(file_dcm, 'SeriesTime') else None,
            "series_description": str(file_dcm.SeriesDescription) if hasattr(file_dcm, 'SeriesDescription') else None,
            "body_part_examined": str(file_dcm.BodyPartExamined) if hasattr(file_dcm, 'BodyPartExamined') else None,
            "modality": str(file_dcm.Modality) if hasattr(file_dcm, 'Modality') else None,
        }
    elif collection == "image":
        return {
            **common_metadata,
            "study_id": str(file_dcm[0x0020, 0x0010].value) if [0x0020, 0x0010] in file_dcm else None,
            "series_number": str(file_dcm.SeriesNumber) if hasattr(file_dcm, 'SeriesNumber') else None,
            "instance_number": str(file_dcm.InstanceNumber) if hasattr(file_dcm, 'InstanceNumber') else None,
            "sop_instance_uid": str(file_dcm.SOPInstanceUID) if hasattr(file_dcm, 'SOPInstanceUID') else None,
            "path": str(pathname),
        }
    return None

def handle_file_dcm(pathname):
    """Handle and process a DICOM file."""
    with _client.start_session() as session:
        try:
            session.start_transaction()
            file_dcm = dcmread(pathname, force=True)

            # Insert patient metadata
            insert_metadata("patient", file_dcm, pathname, session)

            # Insert study metadata
            insert_metadata("study", file_dcm, pathname, session)

            # Insert series metadata
            insert_metadata("series", file_dcm, pathname, session)

            # Insert image metadata
            insert_metadata("image", file_dcm, pathname, session)

            session.commit_transaction()
            LOGGER.info(f"Successfully inserted {pathname}")
        except (InvalidDicomError, OperationFailure) as e:
            session.abort_transaction()
            LOGGER.error(f"Error processing {pathname}: {e}")

def insert_metadata(collection, file_dcm, pathname, session):
    """Helper function to insert DICOM metadata into MongoDB."""
    metadata = generate_metadata(collection, file_dcm, pathname)
    collection_name = _db[collection]
    if metadata:
        collection_name.update_one(
            {key: metadata[key] for key in metadata if key in ["patient_id", "study_id", "series_instance_uid", "sop_instance_uid"]},
            {
                "$set": {**metadata, "updated_at": datetime.now()},
                "$setOnInsert": {"created_at": datetime.now()}
            },
            upsert=True,
            session=session
        )

def dicom_push(pathname):
    """Process a DICOM file push request."""
    time.sleep(3)

    if allowed_file(pathname):
        if pathname.lower().endswith('.dcm'):
            handle_file_dcm(pathname)
    else:
        LOGGER.error("File is not allowed or not a DICOM file.")

def dicom_to_satusehat_task(patient_id, study_id, accession_number, series_number, instance_number):
    """Send DICOM data to Satusehat."""
    LOGGER.info("Processing to Satusehat Task")

    ae = AE(ae_title=config.self_ae_title)
    ae.add_requested_context(Verification)

    assoc = ae.associate('localhost', config.dicom_port, AllStoragePresentationContexts, ae_title=config.self_ae_title)

    if assoc.is_established:
        try:
            image_coll = connect_mongodb("image")
            query = {
                "patient_id": patient_id,
                "study_id": study_id,
                "integration_status_satusehat": {"$ne": 1}
            }
            if series_number is not None:
                query["series_number"] = series_number
            if instance_number is not None:
                query["instance_number"] = instance_number
            study_instances = image_coll.find(query).sort("series_number", 1)

            instance_list = list(study_instances)
            LOGGER.info(f"Instances found: {len(instance_list)}")

            update_integration_status(patient_id, study_id, series_number, instance_number)

            for i, imd in enumerate(instance_list):
                process_instance(imd, patient_id, study_id, series_number, instance_number, assoc, image_coll)

                if i == len(instance_list) - 1:
                    update_integration_summary(patient_id, study_id, accession_number, instance_list, image_coll)

                time.sleep(1)

            LOGGER.info("Process to Satusehat Task finished")
        except Exception as e:
            LOGGER.error(f"Error processing to Satusehat Task: {str(e)}")

        assoc.release()
        LOGGER.info('Association released')
    else:
        LOGGER.info('Association rejected, aborted or never connected')

def update_integration_status(patient_id, study_id, series_number, instance_number):
    """Update the integration status in MongoDB."""
    image_coll = connect_mongodb("image")
    image_coll.update_many({
        "patient_id": patient_id,
        "study_id": study_id,
        "series_number": series_number,
        "instance_number": instance_number,
    }, {
        "$set": {"integration_satusehat_at": datetime.now()}
    })

def process_instance(imd, patient_id, study_id, series_number, instance_number, assoc, image_coll):
    """Process each DICOM instance."""
    LOGGER.info(f"Processing item with instance_number: {imd['instance_number']}")

    path = imd.get('path')
    dcm_data = dcmread(path)
    status = assoc.send_c_store(dcm_data)

    if status.Status == 0x0000:
        LOGGER.info('DICOM file sent successfully')
        update_image_status(image_coll, patient_id, study_id, series_number, instance_number, 1)
    else:
        LOGGER.error('Failed to send DICOM file')
        update_image_status(image_coll, patient_id, study_id, series_number, instance_number, 0)

def update_image_status(image_coll, patient_id, study_id, series_number, instance_number, status):
    """Update the status of the DICOM image in the MongoDB."""
    image_coll.update_one({
        "patient_id": patient_id,
        "study_id": study_id,
        "series_number": series_number,
        "instance_number": instance_number,
    }, {
        "$set": {"integration_status_satusehat": status}
    })

def update_integration_summary(patient_id, study_id, accession_number, instance_list, image_coll):
    """Update the integration summary based on success and failure counts."""
    success_count = image_coll.count_documents({
        "patient_id": patient_id,
        "study_id": study_id,
        "integration_status_satusehat": 1
    })
    failed_count = image_coll.count_documents({
        "patient_id": patient_id,
        "study_id": study_id,
        "integration_status_satusehat": 0
    })

    LOGGER.info(f"Success Count: {success_count}")
    LOGGER.info(f"Failed Count: {failed_count}")

    integration_coll = connect_mongodb("integration")
    integration_coll.update_one({
        "patient_id": patient_id,
        "study_id": study_id,
        "accession_number": accession_number
    }, {
        "$set": {
            "count_success": success_count,
            "count_failed": failed_count,
            "status": "SUCCESS" if success_count == len(instance_list) else "PENDING",
            "message": "[dicom-router] All instances sent successfully" if success_count == len(instance_list) else "[dicom-router] All or some instances failed to send"
        }
    })
