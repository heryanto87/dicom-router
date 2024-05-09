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
from pynetdicom.sop_class import (
  Verification,
)


config.init()

LOGGER = logging.getLogger("flask_server")
ALLOWED_EXTENSIONS = {'dcm', 'zip'}

_client = client_mongodb()
_db = _client[config.pacs_db_name]



def allowed_file(filename):
  return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def generate_metadata(collection, file_dcm, pathname):
  if collection == "patient":
    return {
      "patient_id": str(file_dcm.PatientID) if hasattr(file_dcm, 'PatientID') else None,
      "patient_name": str(file_dcm.PatientName) if hasattr(file_dcm, 'PatientName') else None,
    }
  elif collection == "study":
    return {
      "patient_id": str(file_dcm.PatientID) if hasattr(file_dcm, 'PatientID') else None,
      "study_id": str(file_dcm[0x0020, 0x0010].value) if [0x0020, 0x0010] in file_dcm else None,
      "study_instance_uid": str(file_dcm.StudyInstanceUID) if hasattr(file_dcm, 'StudyInstanceUID') else None,
      "study_date": str(file_dcm.StudyDate) if hasattr(file_dcm, 'StudyDate') else None,
      "study_time": str(file_dcm.StudyTime) if hasattr(file_dcm, 'StudyTime') else None,
      "study_description": str(file_dcm.StudyDescription) if hasattr(file_dcm, 'StudyDescription') else None,
      "accession_number": str(file_dcm.AccessionNumber) if hasattr(file_dcm, 'AccessionNumber') else None,
    }
  elif collection == "series":
    return {
      "patient_id": str(file_dcm.PatientID) if hasattr(file_dcm, 'PatientID') else None,
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
      "patient_id": str(file_dcm.PatientID) if hasattr(file_dcm, 'PatientID') else None,
      "study_id": str(file_dcm[0x0020, 0x0010].value) if [0x0020, 0x0010] in file_dcm else None,
      "series_number": str(file_dcm.SeriesNumber) if hasattr(file_dcm, 'SeriesNumber') else None,
      "instance_number": str(file_dcm.InstanceNumber) if hasattr(file_dcm, 'InstanceNumber') else None,
      "sop_instance_uid": str(file_dcm.SOPInstanceUID) if hasattr(file_dcm, 'SOPInstanceUID') else None,
      # others
      "path": str(pathname),
    }
  else:
    return None

# def handle_file_zip(pathname):
#   try:
#     with zipfile.ZipFile(pathname, 'r') as zip_ref:
#       # Check for a single folder at the top level of the ZIP file
#       top_level_folders = [f for f in zip_ref.namelist() if '/' not in f]

#       if len(top_level_folders) == 1:
#         folder_name = top_level_folders[0]

#         # Extract the folder to a temporary directory
#         temp_dir = 'temp_extracted_folder'
#         zip_ref.extractall(temp_dir)

#         # Read the first DICOM file from the extracted folder
#         read_first_dicom_from_series(os.path.join(temp_dir, folder_name))

#         # Clean up temporary directory
#         os.remove(os.path.join(temp_dir, folder_name))
#         os.rmdir(temp_dir)

#       else:
#         # Read the first DICOM file from the ZIP archive
#         read_first_dicom_from_series(pathname)

#   except zipfile.BadZipFile:
#     print("Invalid ZIP file.")
#   except Exception as e:
#     print(f"Error reading ZIP file: {e}")

def handle_file_dcm(pathname):
  with _client.start_session() as session:
    try:
      session.start_transaction()

      file_dcm = dcmread(pathname, force=True)
      
      # Insert into patient collection
      dcm_metadata_patient = generate_metadata("patient", file_dcm, pathname)
      patient_coll = _db['patient']
      patient = patient_coll.update_one(
          {'patient_id': dcm_metadata_patient["patient_id"]},
          {
              "$set": {**dcm_metadata_patient, "updated_at": datetime.now()},
              "$setOnInsert": {"created_at": datetime.now()}
          },
          upsert=True,
          session=session
      )
      
      # _patient_id = patient.upserted_id or patient.raw_result.get('upserted')

      # Insert into study collection
      dcm_metadata_study = generate_metadata("study", file_dcm, pathname)
      study_coll = _db['study']
      study = study_coll.update_one(
          {
            # "_patient_id": _patient_id,
            'study_id': dcm_metadata_study["study_id"],
            'study_instance_uid': dcm_metadata_study["study_instance_uid"],
            'accession_number': dcm_metadata_study["accession_number"],
          },
          {
              # "$set": {**dcm_metadata_study, "_patient_id": _patient_id, "updated_at": datetime.now()},
              "$set": {**dcm_metadata_study, "updated_at": datetime.now()},
              "$setOnInsert": {"created_at": datetime.now()}
          },
          upsert=True,
          session=session
      )
      
      # _study_id = study.upserted_id or study.raw_result.get('upserted')

      # Insert into series collection
      dcm_metadata_series = generate_metadata("series", file_dcm, pathname)
      series_coll = _db['series']
      series = series_coll.update_one(
          {
            # "_study_id": _study_id,
            'series_number': dcm_metadata_series["series_number"],
            'series_instance_uid': dcm_metadata_series["series_instance_uid"],
          },
          {
              # "$set": {**dcm_metadata_series, "_study_id": _study_id, "updated_at": datetime.now()},
              "$set": {**dcm_metadata_series, "updated_at": datetime.now()},
              "$setOnInsert": {"created_at": datetime.now()}
          },
          upsert=True,
          session=session
      )
      
      # _series_id = series.upserted_id or series.raw_result.get('upserted')

      # Insert into image collection
      dcm_metadata_image = generate_metadata("image", file_dcm, pathname)
      image_coll = _db['image']
      image = image_coll.update_one(
          {
            # "_series_id": _series_id,
            'instance_number': dcm_metadata_image["instance_number"],
            'sop_instance_uid': dcm_metadata_image["sop_instance_uid"],
          },
          {
              # "$set": {**dcm_metadata_image, "_series_id": _series_id, "updated_at": datetime.now()},
              "$set": {**dcm_metadata_image, "updated_at": datetime.now()},
              "$setOnInsert": {"created_at": datetime.now()}
          },
          upsert=True,
          session=session
      )

      # _image_id = image.upserted_id or image.raw_result.get('upserted')

      session.commit_transaction()
      print(f"Successfully inserted {pathname}")
    except (InvalidDicomError, OperationFailure) as e:
      session.abort_transaction()
      print(f"Error inserted {pathname}: {e}")

# def read_first_dicom_from_series(zip_path):
#   with zipfile.ZipFile(zip_path, 'r') as zip_ref:
#     dicom_files = []
#     for file_name in zip_ref.namelist():
#       if file_name.lower().endswith('.dcm'):
#         dicom_files.append(file_name)

#     if not dicom_files:
#       print("No DICOM files found in ZIP archive.")
#       return

#     dicom_files.sort()

#     first_dcm_file = dicom_files[0]
#     with zip_ref.open(first_dcm_file) as dcm_data:
#       try:
#         file_dcm = dcmread(dcm_data, force=True)
#         dcm_metadata = generate_metadata(file_dcm, zip_path)
#         collection = connect_mongodb()
#         collection.insert_one(dcm_metadata)
#         print("Successfully insert one dicom metadata")
#       except InvalidDicomError:
#         print(f"Invalid DICOM file: {first_dcm_file} or missing DICM prefix.")
#       except Exception as e:
#         print(f"Error reading DICOM file {first_dcm_file}: {e}")


# def send_to_dicom_router(dicom_file):    
#     # Read the received DICOM file
#     dcm_data = dcmread(dicom_file)
    
#     # create a DICOM association and send the DICOM message
#     ae = AE(ae_title=config.self_ae_title)
#     ae.add_requested_context(Verification)

#     # assoc = ae.associate('192.168.215.2', config.dicom_port, StoragePresentationContexts, ae_title=config.self_ae_title)
#     assoc = ae.associate('localhost', config.dicom_port, StoragePresentationContexts, ae_title=config.self_ae_title)
#     if assoc.is_established:
#         status = assoc.send_c_store(dcm_data)

#         # Check the status of the storage request
#         if status:
#             # If the storage request succeeded this will be 0x0000, in decimal is 0
#             LOGGER.info('C-STORE request status: 0x{0:04x}'.format(status.Status))
#             if status.Status == 0x0000:
#                 LOGGER.info('DICOM file sent successfully')
#                 return True, status.Status
#         else:
#             LOGGER.info('C-STORE request failed with status: 0x{0:04x}'.format(status.Status))

#         assoc.release()
#         LOGGER.info('Association released')
#         return False, status.Status
#     else:
#         LOGGER.info('Association rejected, aborted or never connected')
#         return False, None

def dicom_push(pathname):
  time.sleep(3)
  # time.sleep(1)

  if allowed_file(pathname):
    if pathname.lower().endswith('.dcm'):
      handle_file_dcm(pathname)

      # # send to dicom router
      # dicom_router_status = send_to_dicom_router(pathname)
      # if dicom_router_status[0]:
      #   print(f"DICOM file sent to DICOM router with status: {dicom_router_status[1]}")
      # else:
      #   print("Failed to establish association with the DICOM router.")

    # elif pathname.lower().endswith('.zip'):
    #   handle_file_zip(pathname)

  else:
    print("File is not allowed or not a DICOM file.")



def dicom_to_satusehat_task(patient_id, study_id, accession_number):
    LOGGER.info(f"Processing to Satusehat Task")
    
    # create a DICOM association and send the DICOM message
    ae = AE(ae_title=config.self_ae_title)
    ae.add_requested_context(Verification)

    assoc = ae.associate('localhost', config.dicom_port, StoragePresentationContexts, ae_title=config.self_ae_title)
    if assoc.is_established:
      try:
          image_coll = connect_mongodb("image")
          study_instances = image_coll.find({
              "patient_id": patient_id,
              "study_id": study_id,
          }).sort("series_number", 1)
          instance_list = list(study_instances)
          LOGGER.info(f"Instances found: {len(instance_list)}")

          for i, imd in enumerate(instance_list):
              LOGGER.info(f"[{i}] - Processing item with instance_number: {str(imd['instance_number'])}; path: {str(imd['path'])}; series_number: {str(imd['series_number'])}")
              
              series_number = imd['series_number'] if 'series_number' in imd else None
              instance_number = imd['instance_number'] if 'instance_number' in imd else None
              path = imd['path'] if 'path' in imd else None

              # Read the DICOM file from path
              dcm_data = dcmread(path)

              # send each instance to DICOM router through send_c_store
              status = assoc.send_c_store(dcm_data)
              # Check the status of the storage request
              if status:
                  # If the storage request succeeded this will be 0x0000, in decimal is 0
                  LOGGER.info('C-STORE request status: 0x{0:04x}'.format(status.Status))
                  if status.Status == 0x0000:
                      LOGGER.info('DICOM file sent successfully')
                      image_coll.update_one({
                        "patient_id": patient_id,
                        "study_id": study_id,
                        "series_number": series_number,
                        "instance_number": instance_number,
                      }, {
                        "$set": {
                          "integration_status_satusehat": 1
                        }
                      })
                  else:
                      LOGGER.info('C-STORE not success')
                      image_coll.update_one({
                        "patient_id": patient_id,
                        "study_id": study_id,
                        "series_number": series_number,
                        "instance_number": instance_number,
                      }, {
                        "$set": {
                          "integration_status_satusehat": 0
                        }
                      })
              else:
                  LOGGER.info('C-STORE request failed with status: 0x{0:04x}'.format(status.Status))
                  image_coll.update_one({
                    "patient_id": patient_id,
                    "study_id": study_id,
                    "series_number": series_number,
                    "instance_number": instance_number,
                  }, {
                    "$set": {
                      "integration_status_satusehat": 0
                    }
                  })


              # wait until last itteration
              if i == len(instance_list) - 1:
                  # count
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
                      "count_failed": failed_count
                    }
                  })
                  # status
                  if success_count == len(instance_list):
                      integration_coll.update_one({
                        "patient_id": patient_id,
                        "study_id": study_id,
                        "accession_number": accession_number
                      }, {
                        "$set": {
                          "status": "SUCCESS",
                          "message": "[dicom-router] All instances sent successfully"
                        }
                      })
                  else:
                      integration_coll.update_one({
                        "patient_id": patient_id,
                        "study_id": study_id,
                        "accession_number": accession_number
                      }, {
                        "$set": {
                          # "status": "FAILED", # or PENDING?because its partially success
                          "status": "PENDING",
                          "message": "[dicom-router] All or some instances failed to send"
                        }
                      })
              time.sleep(1)
          LOGGER.info(f"Process to Satusehat Task finished")
      except Exception as e:
          LOGGER.error(f"Error processing to Satusehat Task: {str(e)}")
      
      assoc.release()
      LOGGER.info('Association released')

    else:
        LOGGER.info('Association rejected, aborted or never connected')