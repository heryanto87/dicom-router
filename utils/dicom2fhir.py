import logging
import os
from fhir import resources as fr
from pydicom import dcmread
from pydicom import dataset
from pydicom import config
from datetime import datetime
from . import fhirutils

config.convert_wrong_length_to_UN = True

LOGGER = logging.getLogger('pynetdicom')

def dcm_coded_concept(CodeSequence):
  concepts = []
  for seq in CodeSequence:
    concept = {}
    concept["code"] = seq[0x0008, 0x0100].value
    concept["system"] = seq[0x0008, 0x0102].value
    concept["display"] = seq[0x0008, 0x0104].value
    concepts.append(concept)
  return concepts


def addInstance(study: fr.imagingstudy.ImagingStudy, series: fr.imagingstudy.ImagingStudySeries, ds: dataset.FileDataset, fp):
  selectedInstance = None
  instanceUID = ds.SOPInstanceUID
  if series.instance is not None:
    selectedInstance = next((i for i in series.instance if i.uid == instanceUID), None)
  else:
    series.instance = []

  if selectedInstance is not None:
    print("Error: SOP Instance UID is not unique")
    print(selectedInstance.as_json())
    return

  init_instance = {
    "uid": "-",
    "sopClass": {
      "system": "urn:ietf:rfc:3986",
      "code": "-"
    }
  }

  selectedInstance = fr.imagingstudy.ImagingStudySeriesInstance(**init_instance)
  selectedInstance.uid = instanceUID
  selectedInstance.sopClass = fhirutils.gen_instance_sopclass(ds.SOPClassUID)
  selectedInstance.number = ds.InstanceNumber
  
  # modified - handle if image type & ConceptNameCodeSequence not found
  try:
    if series.modality.code == "SR":
      seq = ds.ConceptNameCodeSequence
      selectedInstance.title = seq[0x0008, 0x0104]
    else:
      selectedInstance.title = '\\'.join(ds.ImageType)
  except:
    try:
      selectedInstance.title = '\\'.join(ds.ImageType)
    except:
      image_types = ['ORIGINAL', 'PRIMARY']
      selectedInstance.title = '\\'.join(image_types)
    print("Unable to set instance title - Set Default using Image Type: ", selectedInstance.title)

  series.instance.append(selectedInstance)
  study.numberOfInstances = study.numberOfInstances + 1
  series.numberOfInstances = series.numberOfInstances + 1
  return


def addSeries(study: fr.imagingstudy.ImagingStudy, ds: dataset.FileDataset, fp):
  seriesInstanceUID = ds.SeriesInstanceUID
  # TODO: Add test for studyInstanceUID ... another check to make sure it matches
  selectedSeries = None
  if study.series is not None:
    selectedSeries = next((s for s in study.series if s.uid == seriesInstanceUID), None)
  else:
    study.series = []

  if selectedSeries is not None:
    addInstance(study, selectedSeries, ds, fp)
    return
  # Creating New Series

  init_series = {
    "uid": "-",
    "modality": {
      "system": "http://dicom.nema.org/resources/ontology/DCM",
      "code": "-"
    }
  }

  series = fr.imagingstudy.ImagingStudySeries(**init_series)
  series.uid = seriesInstanceUID
  try:
    series.description = ds.SeriesDescription
  except:
    series.description = "No Description"
    print("[Warn] - Series Description not found")

  series.number = ds.SeriesNumber
  series.numberOfInstances = 0

  series.modality = fhirutils.gen_modality_coding(ds.Modality)
  fhirutils.update_study_modality_list(study, series.modality)

  now = datetime.now()
  stime = None
  # modified - if series time not found then set by current time
  try:
    stime = ds.SeriesTime
  except:
    stime = now.strftime("%H%M%S")
  print("Series Date Time : ", stime)

  # modified - if series date not found then set by current date
  try:
    sdate = ds.SeriesDate
    series.started = fhirutils.gen_started_datetime(sdate, stime)
  except:
    sdate = now.strftime("%Y%m%d")
  print("Series Date : ", sdate)

  """
  try:
    series.bodySite = fhirutils.gen_coding_text_only(ds.BodyPartExamined)
  except:
    print("Body Part Examined missing")

  try:
    series.bodySite = fhirutils.gen_coding_text_only(ds.Laterality)
  except:
    print("Laterality missing")
  """

  # TODO: evaluate if we wonat to have inline "performer.actor" for the I am assuming "technician"
  # PerformingPhysicianName	0x81050
  # PerformingPhysicianIdentificationSequence	0x81052

  study.series.append(series)
  study.numberOfSeries = study.numberOfSeries + 1

  addInstance(study, series, ds, fp)
  return


def createImagingStudy(ds, fp, imagingStudyID, serviceRequestID, patientID) -> fr.imagingstudy.ImagingStudy:
  init_data = {
    "status": "available",
    "subject": {
      "reference": "Patient/" + patientID
    },
    "basedOn": [
      {
        "reference": "ServiceRequest/" + serviceRequestID
      }
    ]
  }
  study = fr.imagingstudy.ImagingStudy(**init_data)
  if imagingStudyID != None:
    study.id = imagingStudyID
  study.status = "available"
  try:
    study.description = ds.StudyDescription
  except:
    study.description = "No Description"
    LOGGER.error("Study Description is missing")

  # TODO: ds.IssuerOfAccessionNumberSequence unable to obtain the object and identify correct logic for issuer (SQ)
  study.identifier = []
  study.identifier.append(fhirutils.gen_accession_identifier(ds.AccessionNumber))
  study.identifier.append(fhirutils.gen_studyinstanceuid_identifier(ds.StudyInstanceUID))

  # TODO: ds.IssuerOfPatientID as IPID is not being accepted
  # TODO: ds.PatientBirthTime is also not part of the FileDataset
  study.contained = []
  # patientReference = fr.fhirreference.FHIRReference()
  # patientref = "patient.contained.inline"
  # patientReference.reference = "#" + patientref
  # study.contained.append(fhirutils.inline_patient_resource(
  #     patientref, ds.PatientID, "", ds.PatientName, ds.PatientSex, ds.PatientBirthDate))
  # study.subject = patientReference

  # TODO: at this point we need a correct reference to the directory
  # study.endpoint = []
  # endpoint = fr.fhirreference.FHIRReference()
  # endpoint.reference="file:///" +dcm_dir
  # study.endpoint.append(endpoint)

  procedures = []
  """
  try:
    procedures = dcm_coded_concept(ds.ProcedureCodeSequence)
  except:
    print("Procedure Sequence not found. This is not required FHIR field")
  """

  study.procedureCode = fhirutils.gen_procedurecode_array(procedures)

  studyTime = None
  try:
    studyTime = ds.StudyTime
  except:
    print("Study Time is missing")

  now = datetime.now()
  # modified - if study date not found then use current date
  try:
    studyDate = ds.StudyDate
    study.started = fhirutils.gen_started_datetime(studyDate, studyTime)
  except Exception as e:
    studyDate = now.strftime("%Y%m%d")
    print(e)
    print("Study Date is missing - Set to Current Date : ", studyDate)

  # TODO: we can add "inline" referrer
  # TODO: we can add "inline" reading radiologist.. (interpreter)

  # TODO: untested - could not find dicom sample with Reason for Requested Procedure (0040, 1002) and Seq (0040, 100A) populated
  reason = None
  reasonStr = None
  """
  try:
    reason = dcm_coded_concept(ds.ReasonForRequestedProcedureCodeSequence)
  except:
    print("Reason for Request procedure Code Seq is not available")

  try:
    reasonStr = ds.ReasonForTheRequestedProcedure
  except:
    print("Reason for Requested procedures not found")
  """

  study.reasonCode = fhirutils.gen_reason(reason, reasonStr)
  # end of untested

  # study.numberOfSeries = ds.NumberOfStudyRelatedSeries
  # study.numberOfInstances =ds.NumberOfStudyRelatedInstances

  study.numberOfSeries = 0
  study.numberOfInstances = 0
  addSeries(study, ds, fp)
  return study


def process_dicom_2_fhir(dcm_dir: str, imagingStudyID: str, serviceRequestID: str, patientID: str) -> fr.imagingstudy.ImagingStudy:
  files = []
  for r, d, f in os.walk(dcm_dir):
    for file in f:
      if '.dcm' in file:
        files.append(os.path.join(r, file))

  studyInstanceUID = None
  imagingStudy = None

  try:
    for fp in files:
      # with dcmread(fp, None, [0x7FE00010], force=True) as ds:
      with dcmread(fp, None, False, force=True) as ds:
        if studyInstanceUID is None:
          studyInstanceUID = ds.StudyInstanceUID
        if studyInstanceUID != ds.StudyInstanceUID:
          raise Exception("Incorrect DICOM path, more than one study detected")

        if imagingStudy is None:
          imagingStudy = createImagingStudy(ds, fp, imagingStudyID, serviceRequestID, patientID)
        else:
          addSeries(imagingStudy, ds, fp)
  except Exception as err:
    print(err)

  return imagingStudy
