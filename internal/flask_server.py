from flask import Flask, jsonify, request, send_from_directory
from utils.mongodb import connect_mongodb
from utils import config
from internal.dicom_listener import dicom_push
from urllib.parse import unquote
from flask_cors import CORS
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
