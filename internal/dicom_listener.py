import time
import zipfile
import os
from pydicom import dcmread
from pydicom.errors import InvalidDicomError
from utils.mongodb import connect_mongodb, client_mongodb
from pymongo.errors import OperationFailure
from datetime import datetime

ALLOWED_EXTENSIONS = {'dcm', 'zip'}

_client = client_mongodb()
_db = _client["pacs-live"]

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

def dicom_push(pathname):
  time.sleep(3)
  # time.sleep(1)

  if allowed_file(pathname):
    if pathname.lower().endswith('.dcm'):
      handle_file_dcm(pathname)

    # elif pathname.lower().endswith('.zip'):
    #   handle_file_zip(pathname)

  else:
    print("File is not allowed or not a DICOM file.")

# dicom_push('/home/ponyo/pacs-listen-test/IRSYADUL IBAD.Seq2.Ser201.Img1.dcm')