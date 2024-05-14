from flask import Flask, jsonify, request, send_from_directory, send_file, Response
from utils.mongodb import connect_mongodb, client_mongodb
from utils import config
from internal.dicom_listener import dicom_push, dicom_to_satusehat_task
from urllib.parse import unquote
from flask_cors import CORS
from internal.whatsapp_handler import send
from datetime import datetime
import os
import base64
import threading
import time
import logging

config.init()

LOGGER = logging.getLogger("dicom_router_inotify")

app = Flask(__name__)
CORS(app) # allow all origins


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
  series_number = data['series_number'] if 'series_number' in data else None
  instance_number = data['instance_number'] if 'instance_number' in data else None

  LOGGER.info(f"Process to Satusehat")
  LOGGER.info(f"Patient ID: {patient_id}")
  LOGGER.info(f"Study ID: {study_id}")
  LOGGER.info(f"Accession Number: {accession_number}")

  # if not patient_id or not study_id or not accession_number:
  if not patient_id or not study_id:
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
        t = threading.Thread(target=dicom_to_satusehat_task, args=(patient_id, study_id, accession_number, series_number, instance_number))
        t.start()
        # t.join() # wait until thread is done
        return jsonify({'message': 'Integration data processed'}, 200)
  except Exception as e:
    return jsonify({'message': 'Error sending file to DICOM router: {}'.format(str(e))}, 500)


@app.route('/dicom-upsert', methods=['POST'])
def dicom_upsert():
  _client = client_mongodb()
  _db = _client[config.pacs_db_name]

  data = request.json
  dcm_metadata_patient = data["metadata_patient"]
  dcm_metadata_study = data["metadata_study"]
  dcm_metadata_series = data["metadata_series"]
  dcm_metadata_image = data["metadata_image"]

  with _client.start_session() as session:
    try:
      session.start_transaction()

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

      # Insert into study collection
      study_coll = _db['study']
      study = study_coll.update_one(
          {
            'study_id': dcm_metadata_study["study_id"],
            'study_instance_uid': dcm_metadata_study["study_instance_uid"],
            'accession_number': dcm_metadata_study["accession_number"],
          },
          {
              "$set": {**dcm_metadata_study, "updated_at": datetime.now()},
              "$setOnInsert": {"created_at": datetime.now()}
          },
          upsert=True,
          session=session
      )

      # Insert into series collection
      series_coll = _db['series']
      series = series_coll.update_one(
          {
            'series_number': dcm_metadata_series["series_number"],
            'series_instance_uid': dcm_metadata_series["series_instance_uid"],
          },
          {
              "$set": {**dcm_metadata_series, "updated_at": datetime.now()},
              "$setOnInsert": {"created_at": datetime.now()}
          },
          upsert=True,
          session=session
      )

      # Insert into image collection
      image_coll = _db['image']
      image = image_coll.update_one(
          {
            'instance_number': dcm_metadata_image["instance_number"],
            'sop_instance_uid': dcm_metadata_image["sop_instance_uid"],
          },
          {
              "$set": {**dcm_metadata_image, "updated_at": datetime.now()},
              "$setOnInsert": {"created_at": datetime.now()}
          },
          upsert=True,
          session=session
      )

      session.commit_transaction()
    except Exception as e:
      session.abort_transaction()
  return jsonify({'message': 'Dicom data successfully upserted'}, 200)

@app.route('/dicom-delete', methods=['POST'])
def dicom_delete():
  try:
    # delete patient id from database
    _client = client_mongodb()
    _db = _client[config.pacs_db_name]
    body_data = request.json
    file_query = {'path': body_data['path']}

    image_coll = _db['image']
    result = image_coll.find_one(file_query)
    query = {
      'study_id': result['study_id'],
      'patient_id': result['patient_id']
      }

    result = image_coll.delete_one(query)
    if result.deleted_count == 1:
      LOGGER.info("One document deleted successfully from image collection!")
    else:
      LOGGER.info("No documents matched the query from image collection.")

    study_coll = _db['study']
    result = study_coll.delete_one(query)
    if result.deleted_count == 1:
      LOGGER.info("One document deleted successfully from study collection!")
    else:
      LOGGER.info("No documents matched the query from study collection.")

    series_coll = _db['series']
    result = series_coll.delete_one(query)
    if result.deleted_count == 1:
      LOGGER.info("One document deleted successfully from series collection!")
    else:
      LOGGER.info("No documents matched the query from series collection.")

    return jsonify({'message': 'Successfully delete dicom file'}, 200)
  except Exception as e:
    return jsonify({'message': 'Failed to delete dicom file'}, 500)