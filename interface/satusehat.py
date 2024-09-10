import configparser
import logging
from utils.dbquery import DBQuery
from utils import oauth2
import requests
import os

from utils.dicomutils import make_hash

dbq = DBQuery()
LOGGER = logging.getLogger("pynetdicom")

config = configparser.ConfigParser()
config.read("router.conf")
url = config.get("satusehat", "url")
dicom_pathsuffix = config.get("satusehat", "dicom_pathsuffix")
fhir_pathsuffix = config.get("satusehat", "fhir_pathsuffix")
organization_id = config.get("satusehat", "organization_id")
dcm_dir = config.get("satusehat", "dcm_dir")


def get_service_request(accessionNumber):
  headers = {"Accept": "application/json", "Authorization": "Bearer " + oauth2.token, 'User-Agent': 'PostmanRuntime/7.26.8',}
  path = (
    fhir_pathsuffix
    + "/ServiceRequest?identifier=http://sys-ids.kemkes.go.id/acsn/"
    + organization_id
    + "%7C"
    + accessionNumber
    + "&_sort=-_lastUpdated&_count=1"
  )
  print(headers)
  print(url + path)
  res = requests.get(url=url + path, headers=headers)
  data = res.json()
  if data["resourceType"] == "Bundle" and data["total"] >= 1:
    _, patientID = data["entry"][0]["resource"]["subject"]["reference"].split("/")
    return data["entry"][0]["resource"]["id"], patientID
  raise Exception("ServiceRequest not found")


def get_imaging_study(accessionNumber, token):
  headers = {"Accept": "application/json", "Authorization": "Bearer " + token, 'User-Agent': 'PostmanRuntime/7.26.8',}
  path = (
    fhir_pathsuffix
    + "/ImagingStudy?identifier=http://sys-ids.kemkes.go.id/acsn/"
    + organization_id
    + "%7C"
    + accessionNumber
    + "&_sort=-_lastUpdated&_count=1"
  )
  res = requests.get(url=url + path, headers=headers)
  data = res.json()
  if data["resourceType"] == "Bundle" and data["total"] >= 1:
    _, patientID = data["entry"][0]["resource"]["subject"]["reference"].split("/")
    return data["entry"][0]["resource"]["id"]
  return None


def imagingstudy_post(filename, id):
  headers = {
    "Authorization": "Bearer " + oauth2.token,
    "Content-Type": "application/json",
    'User-Agent': 'PostmanRuntime/7.26.8',
  }
  payload = open(filename, "rb")
  if id == None:
    print(headers)
    print(url + fhir_pathsuffix + "/ImagingStudy")
    res = requests.post(
        url=url + fhir_pathsuffix + "/ImagingStudy", data=payload, headers=headers
    )
  else:
    res = requests.put(
      url=url + fhir_pathsuffix + "/ImagingStudy/" + id,
      data=payload,
      headers=headers,
    )
  data = res.json()
  LOGGER.info(data)
  LOGGER.info(payload)
  if data["resourceType"] == "ImagingStudy":
    return data["id"]
  raise Exception("POST ImagingStudy failed")


def dicom_push(assocId, study_iuid, imagingStudyID):
  if imagingStudyID is None:
      return None
  LOGGER.info("DICOM Push started")
  subdir = make_hash(assocId)
  LOGGER.info("dicom_push imagingStudyID: " + imagingStudyID)
  headers = {
      "Content-Type": "application/dicom",
      "Accept": "application/dicom+json",
      "Authorization": "Bearer " + oauth2.token,
      "X-ImagingStudy-ID": imagingStudyID,
      'User-Agent': 'PostmanRuntime/7.26.8',
  }

  instances = dbq.Query(dbq.GET_INSTANCES_PER_STUDY, [assocId, study_iuid])
  for n in range(len(instances)):
      series_iuid = instances[n][0]
      instance_uid = instances[n][1]
      filename = (
          os.getcwd()
          + dcm_dir
          + subdir
          + "/"
          + study_iuid
          + "/"
          + series_iuid
          + "/"
          + instance_uid
          + ".dcm"
      )
      try:
          payload = open(filename, "rb")
          str = ""
          print("IMAGING STUDY URL:", url + dicom_pathsuffix)
          res = requests.post(
              url=url + dicom_pathsuffix, data=payload, headers=headers
          )
          str = res.text
          LOGGER.info(
              "Sending Instance UID: " + series_iuid + "/" + instance_uid + " success"
          )
          dbq.Update(
              dbq.UPDATE_INSTANCE_STATUS_SENT,
              [assocId, study_iuid, series_iuid, instance_uid],
          )
      except Exception as e:
          LOGGER.error(e)
          LOGGER.error("Sending Instance UID failed: " + instance_uid)
          raise Exception("Sending DICOM failed")

      # output = os.getcwd()+dcm_dir+subdir+ "dicom-push.json"
      # with open(output, 'w') as out:
      #   out.write(str)

      if str.find("Instance already exists") >= 0:
          LOGGER.warning("Image already exists")

          # Remove Instance UID
          os.remove(
              os.getcwd()
              + dcm_dir
              + subdir
              + "/"
              + study_iuid
              + "/"
              + series_iuid
              + "/"
              + instance_uid
              + ".dcm"
          )

          # Remove Series UID Folder if Empty
          os.rmdir(
              os.getcwd() + dcm_dir + subdir + "/" + study_iuid + "/" + series_iuid
          )

  return True


def get_dcm_config(token):
  headers = {"Accept": "application/json", "User-Agent": "PostmanRuntime/7.26.8", "Authorization": "Bearer " + token}
  path = fhir_pathsuffix + "/dcm_cfg"
  res = requests.get(url=url + path, headers=headers)
  data = res.json()
  return data
