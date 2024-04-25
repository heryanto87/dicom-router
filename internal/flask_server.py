from flask import Flask, jsonify, request, send_from_directory
from utils.mongodb import connect_mongodb
from utils import config
from internal.dicom_listener import dicom_push
from urllib.parse import unquote
import os

config.init()

app = Flask(__name__)

@app.route('/hello', methods=['GET'])
def hello():
  return jsonify({'message': 'hello'}, 200)

# API endpoint to retrieve all patients
@app.route('/patients', methods=['GET'])
def get_all_patients():
  collection = connect_mongodb()
  patients = list(collection.distinct('patient_id'))
  # loop through patients and get the latest study
  patients_with_latest_study = []
  for patient in patients:
    latest_study = collection.find_one(
      {'patient_id': patient}, 
      sort=[('study_date', -1), ('study_time', -1)], 
      projection={
        '_id': 0, 
        'instance_number': 0, 
        'series_date': 0, 
        'series_time': 0, 
        'series_description': 0, 
        'series_id': 0,
        'path': 0,
      }
    )
    latest_study['viewer'] = "/viewer?patientId=" + latest_study.get('patient_id') + "&studyId=" + latest_study.get('study_id')
    patients_with_latest_study.append(latest_study)
  return jsonify({'patients': patients_with_latest_study}, 200)


# API endpoint to retrieve all studies for a specific patient
@app.route('/studies/<patientId>', methods=['GET'])
def get_studies_by_patient(patientId):
  collection = connect_mongodb()
  studies = list(collection.distinct('study_id', {'patient_id': patientId}))
  # loop through studies and get the latest series
  studies_with_latest_series = []
  for study in studies:
    latest_series = collection.find_one(
      {'patient_id': patientId, 'study_id': study}, 
      sort=[('series_date', -1), ('series_time', -1)], 
      projection={
        '_id': 0, 
        'instance_number': 0, 
        'series_date': 0, 
        'series_time': 0, 
        'series_description': 0, 
        'series_id': 0,
        'path': 0,
      }
    )
    # add custom field to the latest series
    latest_series['viewer'] = "/viewer?patientId=" + patientId + "&studyId=" + study
    studies_with_latest_series.append(latest_series)
  return jsonify({'studies': studies_with_latest_series}, 200)

# API endpoint to retrieve all series for a specific study of a patient
@app.route('/series/<patientId>/<studyId>', methods=['GET'])
def get_series_by_study(patientId, studyId):
  collection = connect_mongodb()
  series = list(collection.distinct(
    'series_id', 
    {'patient_id': patientId, 'study_id': studyId}
  ))
  # loop through series and get the latest instance
  series_with_latest_instance = []
  for serie in series:
    latest_instance = collection.find_one(
      {'patient_id': patientId, 'study_id': studyId, 'series_id': serie}, 
      sort=[('instance_number', -1)], 
      projection={'_id': 0}
    )
    series_with_latest_instance.append(latest_instance)
  return jsonify({'series': series_with_latest_instance}, 200)

# @app.route('/dicom-files', methods=['GET'])
# def find_all():
#   collection = connect_mongodb()
#   dicom_files = list(collection.find(projection={'_id': 0}))
#   return jsonify({'message': dicom_files}, 200)

# @app.route('/dicom-files/<patientId>', methods=['GET'])
# def find_by_patientId(patientId):
#   # Connect to the MongoDB database
#   collection = connect_mongodb()

#   # Find DICOM files for the patient
#   try:
#     query = {'patient_id': patientId}
#     cursor = collection.find(query, projection={'_id': 0})
#     dicom_files = [doc for doc in cursor]

#     # Check if any files were found
#     if not dicom_files:
#       return jsonify({'message': 'No DICOM files found for patient ID: {}'.format(patientId)}, 404)

#     # Return the list of DICOM files
#     return jsonify(dicom_files), 200

#   except Exception as e:
#     # Handle errors during database interaction
#     return jsonify({'message': 'Error retrieving DICOM files: {}'.format(str(e))}, 500)


# api that resolve dicom files by path
@app.route('/file/resolve', methods=['GET'])
def file_resolve_by_path():
  filename = request.args.get('filename')
  print(f"Filename: {filename}")
  try:
    return send_from_directory(config.inotify_dir, filename, as_attachment=True)
  except FileNotFoundError:
    return "File not found", 404

@app.route('/sync', methods=['GET'])
def sync():
  try:
    folder_path = config.inotify_dir
    if not os.path.isdir(folder_path):
      raise ValueError(f"Invalid folder path: {folder_path}")

    for filename in os.listdir(folder_path):
      if os.path.isfile(os.path.join(folder_path, filename)):
        dicom_push(f'{folder_path}/{filename}')

    return jsonify({'message': 'Successfully sync filesystem'}, 200)

  except Exception as e:
    # Handle errors
    return jsonify({'message': 'Error when syncing filesystem: {}'.format(str(e))}, 500)
