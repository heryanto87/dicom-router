import time
import zipfile
import os
from pydicom import dcmread
from pydicom.errors import InvalidDicomError
from utils.mongodb import connect_mongodb

ALLOWED_EXTENSIONS = {'dcm', 'zip'}

def allowed_file(filename):
  return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def handle_file_zip(pathname):
  try:
    with zipfile.ZipFile(pathname, 'r') as zip_ref:
      # Check for a single folder at the top level of the ZIP file
      top_level_folders = [f for f in zip_ref.namelist() if '/' not in f]

      if len(top_level_folders) == 1:
        folder_name = top_level_folders[0]

        # Extract the folder to a temporary directory
        temp_dir = 'temp_extracted_folder'
        zip_ref.extractall(temp_dir)

        # Read the first DICOM file from the extracted folder
        read_first_dicom_from_series(os.path.join(temp_dir, folder_name))

        # Clean up temporary directory
        os.remove(os.path.join(temp_dir, folder_name))
        os.rmdir(temp_dir)

      else:
        # Read the first DICOM file from the ZIP archive
        read_first_dicom_from_series(pathname)

  except zipfile.BadZipFile:
    print("Invalid ZIP file.")
  except Exception as e:
    print(f"Error reading ZIP file: {e}")

def handle_file_dcm(pathname):
  try:
    file_dcm = dcmread(pathname, force=True)
    dcm_metadata = {
      'patient_id': str(file_dcm.PatientID),
      'patient_name': str(file_dcm.PatientName),
      'study_date': str(file_dcm.StudyDate),
      'body_part_examined': str(file_dcm.BodyPartExamined),
      'modality': str(file_dcm.Modality),
      'path': str(pathname)
    }
    collection = connect_mongodb()
    collection.insert_one(dcm_metadata)
    print(f"Successfully insert {pathname}")
  except InvalidDicomError:
    print("Invalid DICOM file or missing DICM prefix.")
  except Exception as e:
    print(f"Error reading DICOM file: {e}")

def read_first_dicom_from_series(zip_path):
  with zipfile.ZipFile(zip_path, 'r') as zip_ref:
    dicom_files = []
    for file_name in zip_ref.namelist():
      if file_name.lower().endswith('.dcm'):
        dicom_files.append(file_name)

    if not dicom_files:
      print("No DICOM files found in ZIP archive.")
      return

    dicom_files.sort()

    first_dcm_file = dicom_files[0]
    with zip_ref.open(first_dcm_file) as dcm_data:
      try:
        file_dcm = dcmread(dcm_data, force=True)
        dcm_metadata = {
          'patient_id': str(file_dcm.PatientID),
          'patient_name': str(file_dcm.PatientName),
          'study_date': str(file_dcm.StudyDate),
          'body_part_examined': str(file_dcm.BodyPartExamined),
          'modality': str(file_dcm.Modality),
          'path': str(zip_path)
        }
        collection = connect_mongodb()
        collection.insert_one(dcm_metadata)
        print("Successfully insert one dicom metadata")
      except InvalidDicomError:
        print(f"Invalid DICOM file: {first_dcm_file} or missing DICM prefix.")
      except Exception as e:
        print(f"Error reading DICOM file {first_dcm_file}: {e}")

def dicom_push(pathname):
  time.sleep(3)

  if allowed_file(pathname):
    if pathname.lower().endswith('.dcm'):
      handle_file_dcm(pathname)

    elif pathname.lower().endswith('.zip'):
      handle_file_zip(pathname)

  else:
    print("File is not allowed or not a DICOM file.")

# dicom_push('/home/ponyo/pacs-listen-test/IRSYADUL IBAD.Seq2.Ser201.Img1.dcm')