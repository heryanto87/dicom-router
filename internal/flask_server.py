from flask import Flask, jsonify, request, send_from_directory
from utils.mongodb import connect_mongodb
from utils import config
from internal.dicom_listener import dicom_push
from urllib.parse import unquote
from flask_cors import CORS
from internal.whatsapp_handler import send
import os

config.init()

app = Flask(__name__)
CORS(app) # allow all origins

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
