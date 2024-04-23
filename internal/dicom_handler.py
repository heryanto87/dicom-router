import logging
import os
import requests

from pydicom import dcmread
from pydicom.dataset import Dataset, DataElement
from pydicom.uid import generate_uid
from interface import satusehat

from utils.dbquery import dbquery
from utils.findquery import findquery
from utils.dicom2fhir import process_dicom_2_fhir
from utils.dicomutils import make_association_id, make_hash

from utils import oauth2
global token

LOGGER = logging.getLogger('pynetdicom')


# Translate from the element keyword to the db attribute
_TRANSLATION = {
    "PatientID": "patient_id",  # PATIENT | Unique | VM 1 | LO
    "PatientName": "patient_name",  # PATIENT | Required | VM 1 | PN
    "StudyInstanceUID": "study_instance_uid",  # STUDY | Unique | VM 1 | UI
    "StudyDate": "study_date",  # STUDY | Required | VM 1 | DA
    "StudyTime": "study_time",  # STUDY | Required | VM 1 | TM
    "AccessionNumber": "accession_number",  # STUDY | Required | VM 1 | SH
    "StudyID": "study_id",  # STUDY | Required | VM 1 | SH
    "SeriesInstanceUID": "series_instance_uid",  # SERIES | Unique | VM 1 | UI
    "Modality": "modality",  # SERIES | Required | VM 1 | CS
    "SeriesNumber": "series_number",  # SERIES | Required | VM 1 | IS
    "SOPInstanceUID": "sop_instance_uid",  # IMAGE | Unique | VM 1 | UI
    "InstanceNumber": "instance_number",  # IMAGE | Required | VM 1 | IS
}

# Unique and required keys and their level, VR and VM for Patient Root
# Study Root is the same but includes the PATIENT attributes
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
    """Handler for evt.EVT_C_ECHO.

    Parameters
    ----------
    event : events.Event
        The corresponding event.
    cli_config : dict
        A :class:`dict` containing configuration settings passed via CLI.
    logger : logging.Logger
        The application's LOGGER.

    Returns
    -------
    int
        The status of the C-ECHO operation, always ``0x0000`` (Success).
    """
    requestor = event.assoc.requestor
    timestamp = event.timestamp.strftime("%Y-%m-%d %H:%M:%S")
    addr, port = requestor.address, requestor.port
    LOGGER.info(f"Received C-ECHO request from {addr}:{port} at {timestamp}")

    return 0x0000


def handle_store(event, dcm_dir, logger):
    """Handle a C-STORE request event."""
    # Decode the C-STORE request's *Data Set* parameter to a pydicom Dataset
    LOGGER.info("[Info-Assoc] - handle_store")
    
    # Setup database
    dbq = dbquery()

    ds = event.dataset
    ds = ds[0x00030000:]

    # Add the File Meta Information
    ds.file_meta = event.file_meta

    # print("[Info-Assoc] - StudyInstanceUID      : " + ds.StudyInstanceUID)
    # print("[Info-Assoc] - SeriesInstanceUID     : " + ds.SeriesInstanceUID)
    # print("[Info-Assoc] - SOPInstanceUID        : " + ds.SOPInstanceUID)
    # print("[Info-Assoc] - event.assoc.name      : " + event.assoc.name)
    # print("[Info-Assoc] - event.assoc.native_id : " + str(event.assoc.native_id))

    assocId = make_association_id(event)
    subdir = make_hash(assocId)
    subdir = subdir + "/" + ds.StudyInstanceUID + "/" + ds.SeriesInstanceUID + "/"

    try:
        os.makedirs(os.getcwd()+dcm_dir+subdir, exist_ok=True)
        LOGGER.info("Directory created")
    except:
        LOGGER.info("Directory already created")
    filename = os.getcwd()+dcm_dir+subdir+ds.SOPInstanceUID+".dcm"
    ds.save_as(filename, write_like_original=False)

    # insert into db
    scu_ae = event.assoc.requestor.primitive.calling_ae_title
    scp_ae = event.assoc.requestor.primitive.called_ae_title
    entry = (
        make_association_id(event),
        scu_ae,
        scp_ae,
        ds.AccessionNumber,
        ds.StudyInstanceUID,
        ds.SeriesInstanceUID,
        ds.SOPInstanceUID,
        filename)
    try:
        dbq.Insert(dbq.INSERT_SOP, entry)
    except:
        LOGGER.error("Could not insert the entry into database.")

    # Return a 'Success' status
    return 0x0000

def handle_assoc_released(event, dcm_dir, organization_id, mroc_client_url, encrypt, logger):
    """Handle an ASSOCIATION RELEASE event."""

    # Setup database
    dbq = dbquery()

    global token
    token = oauth2.get_token()
    assocId = make_association_id(event)
    try:
        imagingStudyID = None
        dbq.Update(dbq.UPDATE_ASSOC_COMPLETED, [assocId])
        ids = dbq.Query(dbq.GET_IDS_PER_ASSOC, [assocId])
        if len(ids)>0:
            LOGGER.info("Processing DICOM start")
        for stdy in range(len(ids)):
            study_iuid = ids[stdy][0]
            accession_no = ids[stdy][1]
            LOGGER.info("Accession Number: "+accession_no)
            LOGGER.info("Study IUID: "+study_iuid)
            imagingStudyID = satusehat.get_imaging_study(accession_no, token)
            subdir = make_hash(assocId)
            study_dir = os.getcwd()+dcm_dir+subdir+"/"+study_iuid
            serviceRequestID = None
            patientID = None
            try:
                LOGGER.info("Obtaining Patient ID and ServiceRequest ID")
                serviceRequestID, patientID = satusehat.get_service_request(accession_no)
                LOGGER.info("Patient ID and ServiceRequest ID obtained")
            except:
                LOGGER.exception("Failed to obtain Patient ID and ServiceRequest ID")

            # POST files to mroc client backend
            LOGGER.info("Encryption Config is %s", encrypt)
            # if encrypt is not None and encrypt.lower() in ("yes", "true", "t", "1"):
            if encrypt:
                try:
                    print("[Info] - Start POST-ing files to mroc client backend")
                    response = post_files_to_mroc_client(patientID, organization_id, study_dir, accession_no, mroc_client_url)
                    print("[Info] - Response: "+str(response))
                except Exception as e:
                    print(e)
                    print("[Error] - Failed to POST to mroc client backend")

            # Create ImagingStudy
            imagingStudyCreated = False
            if serviceRequestID != None and patientID != None:
                try:
                    LOGGER.info("Start creating ImagingStudy")
                    imagingStudy = process_dicom_2_fhir(
                        study_dir, imagingStudyID, serviceRequestID, patientID)
                    output = study_dir + "/ImagingStudy.json"
                    with open(output, 'w') as out:
                        out.write(imagingStudy.json(indent=2))
                    imagingStudyCreated = True
                    LOGGER.info("ImagingStudy "+study_iuid+" created")
                except Exception as e:
                    LOGGER.error(e)
                    LOGGER.error("Failed to create ImagingStudy for " + study_iuid)

            # POST ImagingStudy
            imagingStudyPosted = False
            if imagingStudyCreated:
                try:
                    imaging_study_json = study_dir + "/ImagingStudy.json"
                    if imagingStudyID == None:
                        LOGGER.info("POST-ing ImagingStudy")
                        id = satusehat.imagingstudy_post(imaging_study_json, None)
                        LOGGER.info("ImagingStudy POST-ed, id: "+id)
                        imagingStudyID = id
                    else:
                        id = satusehat.imagingstudy_post(
                            imaging_study_json, imagingStudyID)
                        LOGGER.info("ImagingStudy already POST-ed, using PUT instead, id: "+id)
                    imagingStudyPosted = True
                except Exception as e:
                    LOGGER.error(e)
                    LOGGER.error("Failed to POST ImagingStudy")

            # Send DICOM
            if imagingStudyPosted:
                try:
                    # comment here for testing without sending to dicom storage
                    satusehat.dicom_push(assocId, study_iuid, imagingStudyID)
                    LOGGER.info("DICOM sent successfully")
                except Exception as e:
                    LOGGER.exception(e)
                    LOGGER.error("[Error] - Failed to send DICOM")

        # Check and delete if all clear
        unsentInstances = False
        instances = dbq.Query(dbq.GET_INSTANCES_PER_ASSOC, [assocId])
        for n in range(len(instances)):
            sent_status = instances[n][3]
            if sent_status == 0:
                unsentInstances = True

        # # Delete if all sent
        # if unsentInstances==False:
        #   LOGGER.info("Deleting association folder")
        #   try:
        #     subdir = make_hash(assocId)
        #     shutil.rmtree(os.getcwd()+dcm_dir+subdir)
        #   except BaseException as e :
        #     print(e)

    except Exception as e:
        LOGGER.error("Could not process association: "+assocId)
        LOGGER.exception(e)

    # Return a 'Success' status
    return 0x0000


def post_files_to_mroc_client(patient_id, organization_id, study_dir, accession_number, mroc_client_url):
    url = mroc_client_url

    # Prepare form data
    data = {
        'patientId': patient_id,
        'organizationId': organization_id,
        'accessionNumber': accession_number
    }

    # Prepare files
    files = []
    for root, dirs, filenames in os.walk(study_dir):
        for filename in filenames:
            file_path = os.path.join(root, filename)
            files.append(('files', (filename, open(file_path, 'rb'), 'application/dicom')))

    print("[Info] - POST-ing files to mroc client backend")
    print("[Info] - URL: "+url)
    print("[Info] - Data: "+str(data))
    print("[Info] - Files: "+str(files))

    try:
        response = requests.post(url=url+"/files", data=data, files=files)
        # response.raise_for_status()  
        print("[Info] - Files POST-ed successfully")
        return response
    except requests.exceptions.RequestException as e:
        print("[Error] - Failed to POST files:", e)
        raise e



def build_query(identifier, session, query=None):
    """Perform a query against the database.

    Parameters
    ----------
    identifier : pydicom.dataset.Dataset
        The request's *Identifier* dataset containing the query attributes.
    session : sqlalchemy.orm.session.Session
        The session we are using to query the database.
    query : sqlalchemy.orm.query.Query, optional
        If not used then start a new query, otherwise extend the existing
        `query`.

    Returns
    -------
    sqlalchemy.orm.query.Query
        The resulting query.
    """
    # VRs for Single Value Matching and Wild Card Matching
    _text_vr = ["AE", "CS", "LO", "LT", "PN", "SH", "ST", "UC", "UR", "UT"]
    for elem in [e for e in identifier if e.keyword in _ATTRIBUTES]:
        vr = elem.VR
        val = elem.value
        # Convert PersonName3 to str
        if vr == "PN" and val:
            val = str(val)

        # Part 4, C.2.2.2.1 Single Value Matching
        if vr != "SQ" and val is not None:
            if vr in _text_vr and ("*" in val or "?" in val):
                pass
            elif vr in ["DA", "TM", "DT"] and "-" in val:
                pass
            else:
                # print('Performing single value matching...')
                query = f"_search_single_value(elem, session, {query})"
                continue

        # Part 4, C.2.2.2.3 Universal Matching
        if val is None:
            # print('Performing universal matching...')
            query = f"_search_universal(elem, session, {query})"
            continue

        # Part 4, C.2.2.2.2 List of UID Matching
        if vr == "UI":
            # print('Performing list of UID matching...')
            query = f"_search_uid_list(elem, session, {query})"
            continue

        # Part 4, C.2.2.2.4 Wild Card Matching
        if vr in _text_vr and ("*" in val or "?" in val):
            # print('Performing wildcard matching...')
            query = f"_search_wildcard(elem, session, {query})"
            continue

        # Part 4, C.2.2.2.5 Range Matching
        if vr in ["DT", "TM", "DA"] and "-" in val:
            query = f"_search_range(elem, session, {query})"
            continue

        # Part 4, C.2.2.2.6 Sequence Matching
        #   No supported attributes are sequences

    return query

def handle_find(event, logger):
    """Handle a C-FIND request event."""
    ds = event.identifier
    ds.to_json_dict()
    
    fq = findquery()
    sql = fq.GenerateSql(ds)
    print("[Info] Generated SQL: ", sql)

    dbq = dbquery()

    # Import stored SOP Instances

    instances = dbq.Query(sql, [])

    LOGGER.info(instances)

    if 'QueryRetrieveLevel' not in ds:
        # Failure
        ds.QueryRetrieveLevel = 'STUDY'

    if ds.QueryRetrieveLevel == 'PATIENT':
        if 'PatientName' in ds:
            if ds.PatientName not in ['*', '', '?']:
                matching = [
                    inst for inst in instances if inst.PatientName == ds.PatientName
                ]

            # Skip the other possible values...

        # Skip the other possible attributes...

    # Skip the other QR levels...

    for instance in instances:
        # Check if C-CANCEL has been received
        if event.is_cancelled:
            yield (0xFE00, None)
            return

        ds = Dataset()

        ds.PatientName = instance["patient_name"]
        ds.PatientID = instance["patient_mrn"]
        ds.PatientBirthDate = instance["patient_birthdate"]
        ds.PatientSex = instance["patient_gender"]

        ds.AccessionNumber = instance["accession_number"]

        ds.RequestAttributesSequence = [Dataset()]

        ds.ScheduledStationAETitle = instance["scheduled_station_ae_title"]
        ds.ReferringPhysicianName = instance["referring_phyisician_name"]
        ds.RequestedProcedureID = instance["requested_procedure_id"]
        ds.StudyInstanceUID = instance["study_iuid"]

        req_step_seq = ds.RequestAttributesSequence
        req_step_seq[0].StudyInstanceUID = instance["study_iuid"]
        req_step_seq[0].Modality = instance["modality"]
        req_step_seq[0].ReferencedStudySequence = []
        req_step_seq[0].AccessionNumber = instance["accession_number"]
        req_step_seq[0].RequestedProcedureID = instance["requested_procedure_id"]
        req_step_seq[0].RequestedProcedureDescription = instance["requested_procedure_description"]
        req_step_seq[0].ScheduledProcedureStepStartDate = instance["scheduled_procedure_step_start_date"]
        req_step_seq[0].ScheduledProcedureStepStartTime = instance["scheduled_procedure_step_start_time"]

        ds.ScheduledProcedureStepSequence = [Dataset()]

        sched_step_seq = ds.ScheduledProcedureStepSequence
        sched_step_seq[0].StudyInstanceUID = instance["study_iuid"]
        sched_step_seq[0].Modality = instance["modality"]
        sched_step_seq[0].ScheduledStationAETitle = instance["scheduled_station_ae_title"]
        sched_step_seq[0].ReferencedStudySequence = []
        sched_step_seq[0].AccessionNumber = instance["accession_number"]
        sched_step_seq[0].RequestedProcedureID = instance["requested_procedure_id"]
        sched_step_seq[0].RequestedProcedureDescription = instance["requested_procedure_description"]
        sched_step_seq[0].ScheduledProcedureStepStartDate = instance["scheduled_procedure_step_start_date"]
        sched_step_seq[0].ScheduledProcedureStepStartTime = instance["scheduled_procedure_step_start_time"]

        # Pending
        yield (0xFF00, ds)
