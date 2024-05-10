from flask import Flask, jsonify, request, send_from_directory, send_file, Response
from utils.mongodb import connect_mongodb
from utils import config
from internal.dicom_listener import dicom_push, dicom_to_satusehat_task
from urllib.parse import unquote
from flask_cors import CORS
from internal.whatsapp_handler import send
import os
import base64
import threading
import time
import logging

config.init()

LOGGER = logging.getLogger("flask_server")
FORMAT = '[%(asctime)s] %(message)s'
logging.basicConfig(filename='flask_server.log', encoding='utf-8', format=FORMAT, level=logging.INFO)

app = Flask(__name__)
CORS(app) # allow all origins


@app.route('/dicom/<patientId>/<studyId>/<seriesNumber>/preview', methods=['GET'])
def dicom_preview(patientId, studyId, seriesNumber):
  try:
    collection = connect_mongodb("series")
    query = {
      "patient_id": patientId,
      "study_id": studyId,
      "series_number": seriesNumber
    }
    result = collection.find_one(query)
    if not result:
      return jsonify({'message': 'Data not found'}, 404)
    if 'preview' not in result:
      return jsonify({'message': 'Preview image not found'}, 404)
    base64_image = result['preview'] 
    image_bytes = base64.b64decode(base64_image)
    return Response(image_bytes, mimetype='image/jpeg')

  except Exception as e:
    return jsonify({'message': 'Error when fetching data: {}'.format(str(e))}, 500)

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

@app.route('/whatsapp', methods=['POST'])
def whatsapp_send():
  data = request.json
  patientPhoneNumber = data['patient_phone_number'] if 'patient_phone_number' in data else None
  previewImage = data['preview_image'] if 'preview_image' in data else None
  patientName = data['patient_name'] if 'patient_name' in data else None
  examination = data['examination'] if 'examination' in data else None
  hospital = data['hospital'] if 'hospital' in data else None
  date = data['date'] if 'date' in data else None
  link = data['link'] if 'link' in data else None

  if not patientPhoneNumber or not previewImage or not patientName or not examination or not hospital or not date or not link:
    return jsonify({'message': 'Invalid request body'}, 400)

  try:
    send(
      patientPhoneNumber, 
      previewImage, 
      patientName, 
      examination, 
      hospital, 
      date, 
      link
    )
    return jsonify({'message': 'Successfully send whatsapp message'}, 200)
  except Exception as e:
    return jsonify({'message': 'Error sending whatsapp message: {}'.format(str(e))}, 500)


@app.route('/to-satusehat', methods=['POST'])
def to_satusehat():
  # get data from request body
  data = request.json
  patient_id = data['patient_id'] if 'patient_id' in data else None
  study_id = data['study_id'] if 'study_id' in data else None
  accession_number = data['accession_number'] if 'accession_number' in data else None

  LOGGER.info(f"Process to Satusehat")
  LOGGER.info(f"Patient ID: {patient_id}")
  LOGGER.info(f"Study ID: {study_id}")
  LOGGER.info(f"Accession Number: {accession_number}")

  if not patient_id or not study_id or not accession_number:
    LOGGER.error("Invalid request body")
    return jsonify({'message': 'Invalid request body'}, 400)
  
  try:
    # check integration data
    integration_coll = connect_mongodb("integration")
    query = {
      "patient_id": patient_id,
      "study_id": study_id,
      "accession_number": accession_number
    }
    integration_data = integration_coll.find_one(query)
    if not integration_data:
      LOGGER.error("Integration data not found")
      return jsonify({'message': 'Integration data not found'}, 404)
    else:
      if integration_data['status'] == 'SUCCESS':
        LOGGER.info("Data already sent to Satu Sehat")
        return jsonify({'message': 'Data already sent to Satu Sehat'}, 200)
      elif integration_data['status'] == 'PENDING' or integration_data['status'] == 'FAILED': # FAILED status will be reprocessed same as PENDING (temporary?)
        LOGGER.info("Integration data status is PENDING and will be processed")
        t = threading.Thread(target=dicom_to_satusehat_task, args=(patient_id,study_id,accession_number,))
        t.start()
        # t.join() # wait until thread is done
        return jsonify({'message': 'Integration data processed'}, 200)
  except Exception as e:
    return jsonify({'message': 'Error sending file to DICOM router: {}'.format(str(e))}, 500)