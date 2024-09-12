from flask import Flask, jsonify, request, send_file
from utils.mongodb import connect_mongodb, client_mongodb
from utils import config
from internal.dicom_listener import dicom_push, dicom_to_satusehat_task
from urllib.parse import unquote
from flask_cors import CORS
from internal.whatsapp_handler import send
from datetime import datetime
import os
import threading
import logging

from dotenv import load_dotenv
load_dotenv()

# Initialize configuration and logger
config.init()
LOGGER = logging.getLogger("dicom_router_inotify")

app = Flask(__name__)
CORS(app)  # Allow all origins

# MongoDB client
_client = client_mongodb()
_db = _client[config.pacs_db_name]

# Helper functions
def validate_request_keys(data, required_keys):
    """Validate the existence of required keys in the request data."""
    missing_keys = [key for key in required_keys if key not in data]
    if missing_keys:
        return jsonify({'message': f'Missing keys: {", ".join(missing_keys)}'}), 400
    return None

@app.route('/sync', methods=['GET'])
def sync():
    """Synchronize DICOM files from the specified folder."""
    try:
        folder_path = config.inotify_dir
        if not os.path.isdir(folder_path):
            raise ValueError(f"Invalid folder path: {folder_path}")

        for filename in os.listdir(folder_path):
            if os.path.isfile(os.path.join(folder_path, filename)):
                dicom_push(f'{folder_path}/{filename}')

        return jsonify({'message': 'Successfully synced filesystem'}), 200
    except Exception as e:
        LOGGER.error(f"Error syncing filesystem: {e}")
        return jsonify({'message': f'Error when syncing filesystem: {str(e)}'}), 500

@app.route('/whatsapp', methods=['POST'])
def whatsapp_send():
    """Send a WhatsApp message."""
    data = request.json
    validation_error = validate_request_keys(data, ['patient_phone_number'])
    if validation_error:
        return validation_error

    try:
        send(
            data.get('patient_phone_number'),
            data.get('preview_image'),
            data.get('patient_name'),
            data.get('examination'),
            data.get('hospital'),
            data.get('date'),
            data.get('link')
        )
        return jsonify({'message': 'Successfully sent WhatsApp message'}), 200
    except Exception as e:
        LOGGER.error(f"Error sending WhatsApp message: {e}")
        return jsonify({'message': f'Error sending WhatsApp message: {str(e)}'}), 500

@app.route('/to-satusehat', methods=['POST'])
def to_satusehat():
    """Process data to Satusehat."""
    data = request.json
    LOGGER.info(f"Processing to Satusehat for patient_id: {data.get('patient_id')}")

    validation_error = validate_request_keys(data, ['patient_id', 'study_id'])
    if validation_error:
        return validation_error

    try:
        integration_coll = connect_mongodb("integration")
        query = {
            "patient_id": data['patient_id'],
            "study_id": data['study_id'],
            "accession_number": data.get('accession_number')
        }
        integration_data = integration_coll.find_one(query)

        if not integration_data:
            LOGGER.error("Integration data not found")
            return jsonify({'message': 'Integration data not found'}), 404
        elif integration_data['status'] == 'SUCCESS':
            LOGGER.info("Data already sent to Satu Sehat")
            return jsonify({'message': 'Data already sent to Satu Sehat'}), 200
        elif integration_data['status'] in ['PENDING', 'FAILED']:
            LOGGER.info("Processing pending integration data")
            threading.Thread(target=dicom_to_satusehat_task, args=(
                data['patient_id'], data['study_id'], data['accession_number'],
                data.get('series_number'), data.get('instance_number')
            )).start()
            return jsonify({'message': 'Integration data processed'}), 200
    except Exception as e:
        LOGGER.error(f"Error processing to Satusehat: {e}")
        return jsonify({'message': f'Error processing to Satusehat: {str(e)}'}), 500

@app.route('/dicom-upsert', methods=['POST'])
def dicom_upsert():
    """Upsert DICOM data."""
    data = request.json
    with _client.start_session() as session:
        try:
            session.start_transaction()
            upsert_collection('patient', data["metadata_patient"], session)
            upsert_collection('study', data["metadata_study"], session)
            upsert_collection('series', data["metadata_series"], session)
            upsert_collection('image', data["metadata_image"], session)
            session.commit_transaction()
            return jsonify({'message': 'DICOM data successfully upserted'}), 200
        except Exception as e:
            session.abort_transaction()
            LOGGER.error(f"Error upserting DICOM data: {e}")
            return jsonify({'message': f'Failed to upsert DICOM data: {str(e)}'}), 500

def upsert_collection(collection_name, metadata, session):
    """Helper function to upsert into a MongoDB collection."""
    collection = _db[collection_name]
    query = {key: metadata[key] for key in metadata if key.endswith('_id') or key.endswith('_uid')}
    collection.update_one(
        query,
        {
            "$set": {**metadata, "updated_at": datetime.now()},
            "$setOnInsert": {"created_at": datetime.now()}
        },
        upsert=True,
        session=session
    )

@app.route('/dicom-delete', methods=['POST'])
def dicom_delete():
    """Delete DICOM data."""
    try:
        data = request.json
        file_query = {'path': data['path']}
        image_coll = _db['image']

        result = image_coll.find_one(file_query)
        query = {'study_id': result['study_id'], 'patient_id': result['patient_id']}

        delete_from_collection(image_coll, query, 'image')
        delete_from_collection(_db['study'], query, 'study')
        delete_from_collection(_db['series'], query, 'series')

        return jsonify({'message': 'Successfully deleted DICOM file'}), 200
    except Exception as e:
        LOGGER.error(f"Error deleting DICOM file: {e}")
        return jsonify({'message': f'Failed to delete DICOM file: {str(e)}'}), 500

def delete_from_collection(collection, query, collection_name):
    """Helper function to delete from a MongoDB collection."""
    result = collection.delete_one(query)
    if result.deleted_count == 1:
        LOGGER.info(f"Document deleted successfully from {collection_name} collection.")
    else:
        LOGGER.info(f"No documents matched the query in {collection_name} collection.")

@app.route('/file/resolve', methods=['GET'])
def file_resolve_by_path():
    """Resolve file by path and serve it."""
    file_path = request.args.get('path')
    if not file_path:
        return "Path not provided", 400

    allowed_directory = '/var/www/lts-temp'
    real_file_path = os.path.realpath(file_path)

    if not os.path.commonpath([allowed_directory, real_file_path]).startswith(allowed_directory):
        return "Unauthorized file access", 403

    if not os.path.isfile(file_path):
        return "File not found", 404

    try:
        return send_file(file_path, as_attachment=True)
    except FileNotFoundError:
        return "File not found", 404
