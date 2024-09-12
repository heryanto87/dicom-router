import logging
import os
import requests

from pydicom import dcmread
from pydicom.dataset import Dataset
from interface import satusehat

from utils.dbquery import DBQuery
from utils.findquery import findquery
from utils.dicom2fhir import process_dicom_2_fhir
from utils.dicomutils import make_association_id, make_hash

from utils import oauth2

from dotenv import load_dotenv
load_dotenv()

# Initialize logger
LOGGER = logging.getLogger('pynetdicom')

# Global token variable
global token

# Translation from element keywords to database attributes
_TRANSLATION = {
    "PatientID": "patient_id",
    "PatientName": "patient_name",
    "StudyInstanceUID": "study_instance_uid",
    "StudyDate": "study_date",
    "StudyTime": "study_time",
    "AccessionNumber": "accession_number",
    "StudyID": "study_id",
    "SeriesInstanceUID": "series_instance_uid",
    "Modality": "modality",
    "SeriesNumber": "series_number",
    "SOPInstanceUID": "sop_instance_uid",
    "InstanceNumber": "instance_number",
}

# Attribute definitions for Patient Root
_ATTRIBUTES = {
    "PatientID": ("PATIENT", "U", "LO", 1),
    "PatientName": ("PATIENT", "R", "PN", 1),
    "StudyInstanceUID": ("STUDY", "U", "UI", 1),
    "StudyDate": ("STUDY", "R", "DA", 1),
    "StudyTime": ("STUDY", "R", "TM", 1),
    "AccessionNumber": ("STUDY", "R", "SH", 1),
    "StudyID": ("STUDY", "R", "SH", 1),
    "SeriesInstanceUID": ("SERIES", "U", "UI", 1),
    "Modality": ("SERIES", "R", "VS", 1),
    "SeriesNumber": ("SERIES", "R", "IS", 1),
    "SOPInstanceUID": ("IMAGE", "U", "UI", 1),
    "InstanceNumber": ("IMAGE", "R", "UI", 1),
}


def handle_echo(event, logger):
    """Handles the C-ECHO request."""
    requestor = event.assoc.requestor
    timestamp = event.timestamp.strftime("%Y-%m-%d %H:%M:%S")
    addr, port = requestor.address, requestor.port
    LOGGER.info(f"Received C-ECHO request from {addr}:{port} at {timestamp}")

    return 0x0000


def handle_store(event, dcm_dir, logger):
    """Handles a C-STORE request event."""
    LOGGER.info("Handling C-STORE request.")

    # Initialize database query instance
    dbq = DBQuery()

    # Get the DICOM dataset from the event and file metadata
    ds = event.dataset
    ds.file_meta = event.file_meta

    # Generate paths and association details
    assocId = make_association_id(event)
    subdir = os.path.join(make_hash(assocId), ds.StudyInstanceUID, ds.SeriesInstanceUID)

    # Ensure the directory exists
    try:
        os.makedirs(os.path.join(dcm_dir, subdir), exist_ok=True)
        LOGGER.info("Directory created successfully.")
    except OSError:
        LOGGER.warning("Directory already exists.")

    filename = os.path.join(dcm_dir, subdir, f"{ds.SOPInstanceUID}.dcm")
    ds.save_as(filename, write_like_original=False)

    # Insert entry into the database
    scu_ae = event.assoc.requestor.primitive.calling_ae_title
    scp_ae = event.assoc.requestor.primitive.called_ae_title
    entry = (
        assocId,
        scu_ae,
        scp_ae,
        ds.AccessionNumber,
        ds.StudyInstanceUID,
        ds.SeriesInstanceUID,
        ds.SOPInstanceUID,
        filename
    )

    try:
        dbq.Insert(dbq.INSERT_SOP, entry)
    except Exception as e:
        LOGGER.error(f"Failed to insert SOP entry into database: {e}")

    return 0x0000


def handle_assoc_released(event, dcm_dir, organization_id, mroc_client_url, encrypt, logger):
    """Handles an ASSOCIATION RELEASE event."""
    dbq = DBQuery()
    global token
    token = oauth2.get_token()

    assocId = make_association_id(event)

    try:
        imagingStudyID = None
        dbq.Update(dbq.UPDATE_ASSOC_COMPLETED, [assocId])
        ids = dbq.Query(dbq.GET_IDS_PER_ASSOC, [assocId])

        if len(ids) > 0:
            LOGGER.info("Processing DICOM files.")

        for study in ids:
            study_iuid, accession_no = study[0], study[1]
            LOGGER.info(f"Accession Number: {accession_no}, Study IUID: {study_iuid}")

            imagingStudyID = satusehat.get_imaging_study(accession_no, token)
            study_dir = os.path.join(dcm_dir, make_hash(assocId), study_iuid)

            # Obtain Patient ID and ServiceRequest ID
            try:
                serviceRequestID, patientID = satusehat.get_service_request(accession_no)
                LOGGER.info("Successfully obtained Patient ID and ServiceRequest ID.")
            except Exception as e:
                LOGGER.error("Failed to obtain Patient ID and ServiceRequest ID.", exc_info=True)
                continue

            # Post files to MROC client backend
            if encrypt:
                try:
                    LOGGER.info("Posting files to MROC client backend.")
                    response = post_files_to_mroc_client(patientID, organization_id, study_dir, accession_no, mroc_client_url)
                    LOGGER.info(f"Response from MROC client: {response}")
                except Exception as e:
                    LOGGER.error(f"Failed to POST to MROC client backend: {e}")

            # Create ImagingStudy
            try:
                imagingStudy = process_dicom_2_fhir(study_dir, imagingStudyID, serviceRequestID, patientID)
                output = os.path.join(study_dir, "ImagingStudy.json")
                with open(output, 'w') as out_file:
                    out_file.write(imagingStudy.json(indent=2))
                LOGGER.info(f"ImagingStudy {study_iuid} created.")
            except Exception as e:
                LOGGER.error(f"Failed to create ImagingStudy for {study_iuid}: {e}")

            # Post ImagingStudy to server
            try:
                imaging_study_json = os.path.join(study_dir, "ImagingStudy.json")
                id = satusehat.imagingstudy_post(imaging_study_json, imagingStudyID or None)
                LOGGER.info(f"ImagingStudy POST-ed successfully, id: {id}")
            except Exception as e:
                LOGGER.error(f"Failed to POST ImagingStudy: {e}")

            # Push DICOM files
            try:
                satusehat.dicom_push(assocId, study_iuid, imagingStudyID)
                LOGGER.info("DICOM files sent successfully.")
            except Exception as e:
                LOGGER.error("Failed to send DICOM files.", exc_info=True)

        # Check if all instances are sent and delete the folder if needed
        unsentInstances = any(inst[3] == 0 for inst in dbq.Query(dbq.GET_INSTANCES_PER_ASSOC, [assocId]))

    except Exception as e:
        LOGGER.error(f"Error processing association {assocId}: {e}", exc_info=True)

    return 0x0000


def post_files_to_mroc_client(patient_id, organization_id, study_dir, accession_number, mroc_client_url):
    """Post files to the MROC client backend."""
    url = f"{mroc_client_url}/files"
    data = {
        'patientId': patient_id,
        'organizationId': organization_id,
        'accessionNumber': accession_number
    }

    files = []
    for root, _, filenames in os.walk(study_dir):
        for filename in filenames:
            file_path = os.path.join(root, filename)
            files.append(('files', (filename, open(file_path, 'rb'), 'application/dicom')))

    try:
        response = requests.post(url=url, data=data, files=files)
        LOGGER.info("Files posted to MROC client successfully.")
        return response
    except requests.exceptions.RequestException as e:
        LOGGER.error(f"Failed to post files: {e}")
        raise


def build_query(identifier, session, query=None):
    """Build a database query from the DICOM identifier."""
    _text_vr = ["AE", "CS", "LO", "LT", "PN", "SH", "ST", "UC", "UR", "UT"]

    for elem in (e for e in identifier if e.keyword in _ATTRIBUTES):
        vr = elem.VR
        val = elem.value

        if vr == "PN" and val:
            val = str(val)

        if vr != "SQ" and val is not None:
            if vr in _text_vr and ("*" in val or "?" in val):
                pass
            elif vr in ["DA", "TM", "DT"] and "-" in val:
                pass
            else:
                query = f"_search_single_value(elem, session, {query})"
        elif val is None:
            query = f"_search_universal(elem, session, {query})"
        elif vr == "UI":
            query = f"_search_uid_list(elem, session, {query})"
        elif vr in _text_vr and ("*" in val or "?" in val):
            query = f"_search_wildcard(elem, session, {query})"
        elif vr in ["DT", "TM", "DA"] and "-" in val:
            query = f"_search_range(elem, session, {query})"

    return query


def handle_find(event, logger):
    """Handles a C-FIND request event."""
    ds = event.identifier
    fq = findquery()
    sql = fq.GenerateSql(ds)

    LOGGER.info(f"Generated SQL query: {sql}")
    dbq = DBQuery()
    instances = dbq.Query(sql, [])

    if 'QueryRetrieveLevel' not in ds:
        ds.QueryRetrieveLevel = 'STUDY'

    if ds.QueryRetrieveLevel == 'PATIENT' and 'PatientName' in ds:
        matching = [inst for inst in instances if inst.PatientName == ds.PatientName]

    for instance in instances:
        if event.is_cancelled:
            yield (0xFE00, None)
            return

        ds = Dataset()
        ds.PatientName = instance["patient_name"]
        ds.PatientID = instance["patient_mrn"]
        ds.PatientBirthDate = instance["patient_birthdate"]
        ds.PatientSex = instance["patient_gender"]
        ds.AccessionNumber = instance["accession_number"]

        # Populate other fields here...

        yield (0xFF00, ds)
