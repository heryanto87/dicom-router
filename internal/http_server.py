import datetime
import json
import logging
import os
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse
from fhir.resources.servicerequest import ServiceRequest
from fhir.resources.patient import Patient
from utils.dbquery import dbquery
from utils import helper

# Initialize configuration
organization_id = os.getenv('ORGANIZATION_ID')

# Initialize logger
LOGGER = logging.getLogger('pynetdicom')

# Common response templates
response_templates = {
    "all_ok": {
        "resourceType": "OperationOutcome",
        "issue": [{
            "severity": "information",
            "code": "informational",
            "details": {"text": "All OK"}
        }]
    },
    "404": {
        "resourceType": "OperationOutcome",
        "issue": [{
            "severity": "error",
            "code": "not-found",
            "details": {"text": "Path not found"}
        }]
    },
    "invalid_resource": {
        "resourceType": "OperationOutcome",
        "issue": [{
            "severity": "error",
            "code": "invalid",
            "details": {"text": "Invalid Resource Type"}
        }]
    },
    "operation_outcome": {
        "resourceType": "OperationOutcome",
        "issue": [{
            "severity": "",
            "code": "",
            "details": {"text": ""}
        }]
    }
}

class HTTPHandler(BaseHTTPRequestHandler):
    def _set_response(self, status_code=200, content_type='application/json', response_body=None):
        self.send_response(status_code)
        self.send_header('Content-type', content_type)
        self.end_headers()
        if response_body:
            self.wfile.write(json.dumps(response_body).encode('utf-8'))

    def do_GET(self):
        parsed_path = urlparse(self.path)
        path_segments = parsed_path.path.strip('/').split('/')

        if not path_segments or path_segments[0] == '':
            self._set_response(response_body=response_templates["all_ok"])
        elif path_segments[0] == 'fhir':
            self._set_response(response_body=response_templates["all_ok"])
        else:
            self._set_response(404, response_body=response_templates["404"])

    def do_POST(self):
        dbq_instance = dbquery()
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length).decode('utf-8')

        # Parse URL path and resource type
        parsed_path = urlparse(self.path)
        path_segments = parsed_path.path.strip('/').split('/')
        resource_type = path_segments[1] if len(path_segments) > 1 else ''

        # Parse JSON payload
        try:
            json_data = json.loads(post_data)
        except json.JSONDecodeError as e:
            response = {'error': 'Invalid JSON format', 'details': str(e)}
            self._set_response(400, response_body=response)
            return

        # Handle ServiceRequest POST request
        if resource_type == "ServiceRequest":
            self.handle_service_request(json_data, dbq_instance)
        else:
            self._set_response(400, response_body=response_templates["invalid_resource"])

    def handle_service_request(self, json_data, dbq_instance):
        """Handles the ServiceRequest resource processing."""
        try:
            sr = ServiceRequest(**json_data)
        except ValueError as e:
            LOGGER.error("Error parsing ServiceRequest", exc_info=True)
            self._send_operation_outcome("error", "invalid", f"{e}", 400)
            return

        patient_data = self.extract_patient_data(sr)
        if patient_data is None:
            return

        accession_number, study_iuid = self.extract_study_data(sr, dbq_instance)
        if study_iuid is None:
            return

        modality, scheduled_station_ae_title, referring_phyisician_name, procedure_data = self.extract_order_details(sr)

        # Insert into MWL and patient table
        try:
            self.insert_work_list(dbq_instance, study_iuid, accession_number, patient_data, modality,
                                  scheduled_station_ae_title, referring_phyisician_name, procedure_data, sr)
            self.insert_patient_data(dbq_instance, patient_data)
        except Exception as e:
            LOGGER.error("Error inserting data", exc_info=True)
            return

        # Add study instance UID to resource identifier
        sr.identifier.append({
            "use": "official",
            "system": "urn:dicom:uid",
            "value": f"urn:oid:{study_iuid}"
        })

        self._set_response(response_body=sr.dict())

    def extract_patient_data(self, sr):
        """Extract patient data from the ServiceRequest resource."""
        try:
            for contained in sr.contained:
                if isinstance(contained, Patient):
                    patient_mrn = next(
                        (id.value for id in contained.identifier if id.system == f"http://sys-ids.kemkes.go.id/mrn/{organization_id}"),
                        None
                    )
                    return {
                        'id': contained.id,
                        'name': contained.name[0].text,
                        'birthDate': contained.birthDate.strftime("%Y%m%d"),
                        'gender': "M" if contained.gender == "male" else "F",
                        'mrn': patient_mrn
                    }
        except Exception as e:
            LOGGER.error(f"Error parsing patient data: {e}")
            self._send_operation_outcome("error", "value", f"Error parsing patient data: {e}", 400)
            return None

        self._send_operation_outcome("error", "value", "Patient resource not found in 'contained'", 400)
        return None

    def extract_study_data(self, sr, dbq_instance):
        """Extracts accession number and study instance UID."""
        accession_number = next(
            (id.value for id in sr.identifier if id.system == f"http://sys-ids.kemkes.go.id/acsn/{organization_id}"),
            None
        )
        study_iuid = next(
            (id.value.strip(':').split(':')[2] for id in sr.identifier if id.system == "urn:dicom:uid"),
            helper.new_study_iuid(organization_id)
        )

        return accession_number, study_iuid

    def extract_order_details(self, sr):
        """Extracts modality, AE title, and referring physician from the ServiceRequest."""
        modality, scheduled_station_ae_title, referring_phyisician_name = "", "", ""

        try:
            modality = next(
                (coding.code for detail in sr.orderDetail for coding in detail.coding
                 if coding.system == "http://dicom.nema.org/resources/ontology/DCM"),
                ""
            )
            scheduled_station_ae_title = next(
                (coding.display for detail in sr.orderDetail for coding in detail.coding
                 if coding.system == "http://sys-ids.kemkes.go.id/ae-title"),
                ""
            )
            referring_phyisician_name = sr.requester.display if sr.requester else ""
        except Exception as e:
            LOGGER.error(f"Error extracting order details: {e}", exc_info=True)
            self._send_operation_outcome("error", "value", "Error extracting order details", 400)

        return modality, scheduled_station_ae_title, referring_phyisician_name, {
            'procedure_id': sr.code.coding[0].code,
            'procedure_description': sr.code.coding[0].display
        }

    def insert_work_list(self, dbq_instance, study_iuid, accession_number, patient_data, modality,
                         scheduled_station_ae_title, referring_phyisician_name, procedure_data, sr):
        """Insert data into MWL table."""
        entry = (
            study_iuid,
            accession_number,
            study_iuid,
            patient_data['id'],
            modality,
            scheduled_station_ae_title,
            referring_phyisician_name,
            procedure_data['procedure_id'],
            procedure_data['procedure_description'],
            sr.occurrenceDateTime.strftime("%Y%m%d"),
            sr.occurrenceDateTime.strftime("%H%M%S")
        )
        dbq_instance.Insert(dbq_instance.INSERT_MWL, entry)

    def insert_patient_data(self, dbq_instance, patient_data):
        """Insert patient data into the patient table."""
        entry = (
            patient_data['id'],
            patient_data['mrn'],
            patient_data['name'],
            patient_data['birthDate'],
            patient_data['gender']
        )
        dbq_instance.Insert(dbq_instance.INSERT_PATIENT, entry)

    def _send_operation_outcome(self, severity, code, details, status_code=400):
        """Helper function to send an OperationOutcome response."""
        response = copy.deepcopy(response_templates["operation_outcome"])
        response['issue'][0]['severity'] = severity
        response['issue'][0]['code'] = code
        response['issue'][0]['details']['text'] = details
        self._set_response(status_code, response_body=response)

def start_server(port):
    """Start the HTTP server."""
    server_address = ('', port)
    httpd = HTTPServer(server_address, HTTPHandler)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        exit()
