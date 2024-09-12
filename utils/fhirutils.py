from datetime import datetime
import pytz
import os

from dotenv import load_dotenv
load_dotenv()

from fhir.resources import imagingstudy, identifier, codeableconcept, coding, patient, humanname, fhirtypes

# Constants
TERMINOLOGY_CODING_SYS = "http://terminology.hl7.org/CodeSystem/v2-0203"
TERMINOLOGY_CODING_SYS_CODE_ACCESSION = "ACSN"
TERMINOLOGY_CODING_SYS_CODE_MRN = "MR"

ACQUISITION_MODALITY_SYS = "http://dicom.nema.org/resources/ontology/DCM"
ACSN_IDENTIFIER_SYSTEM_PREFIX = "http://sys-ids.kemkes.go.id/acsn/"
SOP_CLASS_SYS = "urn:ietf:rfc:3986"

# Load configuration
tz_name = os.getenv('TZ_NAME')
organization_id = os.getenv('ORGANIZATION_ID')

def gen_accession_identifier(id: str) -> identifier.Identifier:
    """Generate an accession identifier."""
    idf = identifier.Identifier()
    idf.use = "usual"
    idf.type = codeableconcept.CodeableConcept(coding=[coding.Coding(system=TERMINOLOGY_CODING_SYS, code=TERMINOLOGY_CODING_SYS_CODE_ACCESSION)])
    idf.value = id
    idf.system = f"{ACSN_IDENTIFIER_SYSTEM_PREFIX}{organization_id}"
    return idf


def gen_studyinstanceuid_identifier(id: str) -> identifier.Identifier:
    """Generate a StudyInstanceUID identifier."""
    return identifier.Identifier(system="urn:dicom:uid", value=f"urn:oid:{id}")


def get_patient_resource_identifications(PatientID: str, IssuerOfPatientID: str) -> identifier.Identifier:
    """Generate patient resource identification."""
    idf = identifier.Identifier()
    idf.use = "usual"
    idf.type = codeableconcept.CodeableConcept(coding=[coding.Coding(system=TERMINOLOGY_CODING_SYS, code=TERMINOLOGY_CODING_SYS_CODE_MRN)])
    idf.system = f"urn:oid:{IssuerOfPatientID}"
    idf.value = PatientID
    return idf


def calc_gender(gender: str) -> str:
    """Convert gender code to FHIR-compatible value."""
    if gender is None or not gender:
        return "unknown"
    gender = gender.lower()
    return {"f": "female", "m": "male", "o": "other"}.get(gender, "unknown")


def calc_dob(dicom_dob: str) -> fhirtypes.Date:
    """Convert DICOM date of birth to FHIR format."""
    if not dicom_dob:
        return None
    try:
        dob = datetime.strptime(dicom_dob, '%Y%m%d')
        return fhirtypes.Date(dob)
    except ValueError:
        return None


def inline_patient_resource(reference_id: str, PatientID: str, IssuerOfPatientID: str, patient_name, gender: str, dob: str) -> patient.Patient:
    """Generate inline FHIR Patient resource."""
    p = patient.Patient()
    p.id = reference_id
    p.identifier = [get_patient_resource_identifications(PatientID, IssuerOfPatientID)]
    hn = humanname.HumanName(family=patient_name.family_name, given=[patient_name.given_name])
    p.name = [hn]
    p.gender = calc_gender(gender)
    p.birthDate = calc_dob(dob)
    p.active = True
    return p


def gen_procedurecode_array(procedures: list) -> list:
    """Generate an array of FHIR procedure codes."""
    if not procedures:
        return None

    fhir_proc = []
    for p in procedures:
        concept = codeableconcept.CodeableConcept(coding=[coding.Coding(system=p["system"], code=p["code"], display=p["display"])])
        concept.text = p["display"]
        fhir_proc.append(concept)

    return fhir_proc if fhir_proc else None


def gen_started_datetime(dt: str, tm: str) -> fhirtypes.DateTime:
    """Generate FHIR DateTime from DICOM date and time."""
    if not dt:
        return None
    dttm = datetime.strptime(dt, '%Y%m%d')
    fhir_dtm = fhirtypes.DateTime(year=dttm.year, month=dttm.month, day=dttm.day)

    if tm and len(tm) >= 6:
        fhirtime = datetime.strptime(tm[:6], '%H%M%S')
        local_tz = pytz.timezone(tz_name)
        local_time = datetime.now(local_tz)
        fhir_dtm = fhirtypes.DateTime(
            year=dttm.year, month=dttm.month, day=dttm.day,
            hour=fhirtime.hour, minute=fhirtime.minute, second=fhirtime.second,
            tzinfo=local_time.tzinfo
        )
    return fhir_dtm


def gen_reason(reason: list, reason_str: str) -> list:
    """Generate FHIR reason code."""
    if not reason and not reason_str:
        return None

    reason_list = []
    if not reason:
        rc = codeableconcept.CodeableConcept(text=reason_str)
        reason_list.append(rc)
        return reason_list

    for r in reason:
        rc = codeableconcept.CodeableConcept(
            coding=[coding.Coding(system=r["system"], code=r["code"], display=r["display"])]
        )
        reason_list.append(rc)

    return reason_list


def gen_modality_coding(mod: str) -> coding.Coding:
    """Generate modality coding."""
    return coding.Coding(system=ACQUISITION_MODALITY_SYS, code=mod)


def update_study_modality_list(study: imagingstudy.ImagingStudy, modality: coding.Coding):
    """Update the modality list for a study."""
    if not study.modality:
        study.modality = [modality]
        return

    if not any(mc.system == modality.system and mc.code == modality.code for mc in study.modality):
        study.modality.append(modality)


def gen_instance_sopclass(SOPClassUID: str) -> coding.Coding:
    """Generate SOP class coding."""
    return coding.Coding(system=SOP_CLASS_SYS, code=f"urn:oid:{SOPClassUID}")


def gen_coding_text_only(text: str) -> coding.Coding:
    """Generate coding with text only."""
    return coding.Coding(code=text, userSelected=True)
