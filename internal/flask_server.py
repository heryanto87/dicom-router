from flask import Flask, jsonify, request
from utils.mongodb import connect_mongodb
from utils import config
from internal.dicom_listener import dicom_push
import os

config.init()

app = Flask(__name__)

@app.route('/hello', methods=['GET'])
def hello():
  return jsonify({'message': 'hello'}, 200)

@app.route('/dicom-files', methods=['GET'])
def find_all():
  collection = connect_mongodb()
  dicom_files = list(collection.find(projection={'_id': 0}))
  return jsonify({'message': dicom_files}, 200)

@app.route('/dicom-files/<patientId>', methods=['GET'])
def find_by_patientId(patientId):
  # Connect to the MongoDB database
  collection = connect_mongodb()

  # Find DICOM files for the patient
  try:
    query = {'patient_id': patientId}
    cursor = collection.find(query, projection={'_id': 0})
    dicom_files = [doc for doc in cursor]

    # Check if any files were found
    if not dicom_files:
      return jsonify({'message': 'No DICOM files found for patient ID: {}'.format(patientId)}, 404)

    # Return the list of DICOM files
    return jsonify(dicom_files), 200

  except Exception as e:
    # Handle errors during database interaction
    return jsonify({'message': 'Error retrieving DICOM files: {}'.format(str(e))}, 500)

@app.route('/sync', methods=['GET'])
def sync():
  try:
    folder_path = config.inotify_dir
    if not os.path.isdir(folder_path):
      raise ValueError(f"Invalid folder path: {folder_path}")

    files = []
    for filename in os.listdir(folder_path):
      # Check if it's a file (not a directory) using os.path.isfile()
      if os.path.isfile(os.path.join(folder_path, filename)):
        dicom_push(f'{folder_path}/{filename}')

    return jsonify({'message': 'Successfully sync filesystem'}, 200)

  except Exception as e:
    # Handle errors
    return jsonify({'message': 'Error when syncing filesystem: {}'.format(str(e))}, 500)
