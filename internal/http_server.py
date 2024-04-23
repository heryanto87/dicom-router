import configparser
import copy
import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import logging
from urllib.parse import parse_qs, urlparse

from fhir.resources.servicerequest import ServiceRequest
from fhir.resources.patient import Patient

from utils.dbquery import dbquery
from utils import helper

config = configparser.ConfigParser()
config.read('router.conf')
organization_id = config.get('satusehat', 'organization_id')

LOGGER = logging.getLogger('pynetdicom')

response_all_ok = {
    "resourceType": "OperationOutcome",
    "issue": [
        {
            "severity": "information",
            "code": "informational",
            "details": {
                "text": "All OK"
            }
        }
    ]
}
response_404 = {
    "resourceType": "OperationOutcome",
    "issue": [
        {
            "severity": "error",
            "code": "not-found",
            "details": {
                "text": "Path not found"
            }
        }
    ]
}
response_invalid_resource = {
    "resourceType": "OperationOutcome",
    "issue": [
        {
            "severity": "error",
            "code": "invalid",
            "details": {
                "text": "Invalid Resource Type"
            }
        }
    ]
}

response_operation_outcome = {
    "resourceType": "OperationOutcome",
    "issue": [
        {
            "severity": "",
            "code": "",
            "details": {
                "text": ""
            }
        }
    ]
}


class HTTPHandler(BaseHTTPRequestHandler):
    def _set_response(self, status_code=200, content_type='application/json'):
        self.send_response(status_code)
        self.send_header('Content-type', content_type)
        self.end_headers()

    def do_GET(self):
        parsed_path = urlparse(self.path)
        path_segments = parsed_path.path.strip('/').split('/')

        end_point = path_segments[0] if path_segments else ''
        resource_type_path = path_segments[1] if path_segments[1] else ''
        path_suffix = path_segments[2:] if len(path_segments) > 2 else []

        if end_point == '':
            self._set_response()
            self.wfile.write(json.dumps(response_all_ok).encode('utf-8'))
        elif end_point == 'fhir':
            self._set_response()
            self.wfile.write(json.dumps(response_all_ok).encode('utf-8'))
        else:
            self._set_response(404)
            self.wfile.write(json.dumps(response_404).encode('utf-8'))

    def do_POST(self):
        dbq = dbquery()

        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length).decode('utf-8')

        # parse url path, get resourceType and suffix if any
        parsed_path = urlparse(self.path)
        path_segments = parsed_path.path.strip('/').split('/')
        end_point = path_segments[0] if path_segments else ''
        resource_type_path = path_segments[1] if path_segments[1] else ''
        path_suffix = path_segments[2:] if len(path_segments) > 2 else []
        json_data = None

        # parse json payload
        try:
            json_data = json.loads(post_data)
        except json.JSONDecodeError as e:
            self._set_response(400, 'application/json')
            response = {'error': 'Invalid JSON format', 'details': str(e)}
            self.wfile.write(json.dumps(response).encode('utf-8'))
            return

        # logic for ServiceRequest
        if resource_type_path == "ServiceRequest":
            sr = None
            try:
                sr = ServiceRequest(**json_data)
            except ValueError as e:
                response = {
                    "resourceType": "OperationOutcome",
                    "issue": [
                        {
                            "severity": "error",
                            "code": "invalid",
                            "details": {
                                "text": f"{e}"
                            }
                        }
                    ]
                }
                self._set_response(400)
                self.wfile.write(json.dumps(response).encode('utf-8'))
                LOGGER.exception(e)
                LOGGER.error("Error parsing ServiceRequest")
                return
            
            # LOGGER.info(f"Generated study instance uid: {study_iuid}")

            accession_number = None
            modality = ""
            scheduled_station_ae_title = ""
            referring_phyisician_name = ""
            requested_procedure_id = ""
            requested_procedure_description = ""
            scheduled_procedure_step_start_date = ""
            scheduled_procedure_step_start_time = ""
            patient_name = ""
            patient_birthdate = ""
            patient_gender = ""
            patient_is_contained = False
            patient_id = ""
            patient_mrn = ""
            study_iuid = None

            try:
                for contained_item in sr.contained:
                    if isinstance(contained_item, Patient):
                      patient_id = contained_item.id
                      patient_name = contained_item.name[0].text
                      patient_birthdate = contained_item.birthDate.strftime("%Y%m%d")
                      patient_gender = "M" if contained_item.gender == "male" else "F" if contained_item.gender == "female" else ""
                      try:
                          for identifier in contained_item.identifier:
                              if identifier.system == f"http://sys-ids.kemkes.go.id/mrn/{organization_id}":
                                  patient_mrn = identifier.value
                                  break
                      except Exception as e:
                          LOGGER.error(f"error here {e}")
                          response = response_operation_outcome.copy()
                          response['issue'][0]['severity'] = "error"
                          response['issue'][0]['code'] = "value"
                          response['issue'][0]['details']['text'] = f"Error parsing patient_mrn in Patient.identifier element: {e}"
                          self._set_response(400)
                          self.wfile.write(json.dumps(response).encode('utf-8'))
                          return
                      LOGGER.info(f"{patient_name} {patient_birthdate} {patient_gender}")
                      patient_is_contained = True
                      break
            except Exception as e:
                LOGGER.error(f"error here {e}")
                response = response_operation_outcome.copy()
                response['issue'][0]['severity'] = "error"
                response['issue'][0]['code'] = "value"
                response['issue'][0]['details']['text'] = f"Error parsing 'contained' element: {e}"
                self._set_response(400)
                self.wfile.write(json.dumps(response).encode('utf-8'))
                return
            
            if not patient_is_contained:
                response = response_operation_outcome.copy()
                response['issue'][0]['severity'] = "error"
                response['issue'][0]['code'] = "value"
                response['issue'][0]['details']['text'] = "contained element must have Patient resourceType"
                self._set_response(400)
                self.wfile.write(json.dumps(response).encode('utf-8'))
                return
                
            if patient_name=="":
                response = response_operation_outcome.copy()
                response['issue'][0]['severity'] = "error"
                response['issue'][0]['code'] = "value"
                response['issue'][0]['details']['text'] = "Patient name is empty"
                self._set_response(400)
                self.wfile.write(json.dumps(response).encode('utf-8'))
                return

            if patient_mrn=="":
                response = response_operation_outcome.copy()
                response['issue'][0]['severity'] = "error"
                response['issue'][0]['code'] = "value"
                response['issue'][0]['details']['text'] = "Patient mrn is empty"
                self._set_response(400)
                self.wfile.write(json.dumps(response).encode('utf-8'))
                return

            # extract accession number and study_iuid if any
            for identifier in sr.identifier:
                if identifier.system == f"http://sys-ids.kemkes.go.id/acsn/{organization_id}":
                    accession_number = identifier.value
                if identifier.system == "urn:dicom:uid":
                    iuid_segments = identifier.value.strip(':').split(':')
                    study_iuid = iuid_segments[2] or helper.new_study_iuid(organization_id)

            # extract patient id
            try:
                patient_id_reference = sr.subject.reference.strip('/').split('/')[1]
            except Exception as e:
                response = response_operation_outcome.copy()
                response['issue'][0]['severity'] = "error"
                response['issue'][0]['code'] = "value"
                response['issue'][0]['details']['text'] = "Cannot parse subject reference"
                self._set_response(400)
                self.wfile.write(json.dumps(response).encode('utf-8'))
                return
            
            if patient_id!=patient_id_reference:
                response = response_operation_outcome.copy()
                response['issue'][0]['severity'] = "error"
                response['issue'][0]['code'] = "value"
                response['issue'][0]['details']['text'] = "Patient id mismatch"
                self._set_response(400)
                self.wfile.write(json.dumps(response).encode('utf-8'))
                return
                
            
            # extract modality code
            try:
              for valueCodeableConcept in sr.orderDetail:
                  for coding in valueCodeableConcept.coding:
                    if coding.system == "http://dicom.nema.org/resources/ontology/DCM":
                        modality = coding.code
                        break
            except Exception as e:
                response = response_operation_outcome.copy()
                response['issue'][0]['severity'] = "error"
                response['issue'][0]['code'] = "value"
                response['issue'][0]['details']['text'] = f"Cannot retrieve modality: {e}"
                self._set_response(400)
                self.wfile.write(json.dumps(response).encode('utf-8'))
                return
            
            # extract ae title
            try:
              for valueCodeableConcept in sr.orderDetail:
                  for coding in valueCodeableConcept.coding:
                    if coding.system == "http://sys-ids.kemkes.go.id/ae-title":
                        scheduled_station_ae_title = coding.display
                        break
            except Exception as e:
                response = response_operation_outcome.copy()
                response['issue'][0]['severity'] = "error"
                response['issue'][0]['code'] = "value"
                response['issue'][0]['details']['text'] = f"Cannot retrieve modality: {e}"
                self._set_response(400)
                self.wfile.write(json.dumps(response).encode('utf-8'))
                return
            
            # extract referring physician
            try:
                referring_phyisician_name = sr.requester.display
            except Exception as e:
                response = response_operation_outcome.copy()
                response['issue'][0]['severity'] = "error"
                response['issue'][0]['code'] = "value"
                response['issue'][0]['details']['text'] = f"Cannot retrieve referring physician: {e}"
                self._set_response(400)
                self.wfile.write(json.dumps(response).encode('utf-8'))
                return

            # extract procedure code and description
            try:
                requested_procedure_id = sr.code.coding[0].code
                requested_procedure_description = sr.code.coding[0].display
            except Exception as e:
                response = response_operation_outcome.copy()
                response['issue'][0]['severity'] = "error"
                response['issue'][0]['code'] = "value"
                response['issue'][0]['details']['text'] = f"Cannot retrieve procedure code: {e}"
                self._set_response(400)
                self.wfile.write(json.dumps(response).encode('utf-8'))
                return
            
            # extract occurrenceDateTime
            try:
                scheduled_procedure_step_start_date = sr.occurrenceDateTime.strftime("%Y%m%d")
                scheduled_procedure_step_start_time = sr.occurrenceDateTime.strftime("%H%M%S")
            except Exception as e:
                response = response_operation_outcome.copy()
                response['issue'][0]['severity'] = "error"
                response['issue'][0]['code'] = "value"
                response['issue'][0]['details']['text'] = f"Cannot parse occurrenceDateTime: {e}"
                self._set_response(400)
                self.wfile.write(json.dumps(response).encode('utf-8'))
                return
            
            if study_iuid==None:
                study_iuid = helper.new_study_iuid(organization_id)

            # insert to work_list table
            entry = (
                study_iuid,
                accession_number,
                study_iuid,
                patient_id,
                modality,
                scheduled_station_ae_title,
                referring_phyisician_name,
                requested_procedure_id,
                requested_procedure_description,
                scheduled_procedure_step_start_date,
                scheduled_procedure_step_start_time,
            )
            try:
                dbq.Insert(dbq.INSERT_MWL, entry)
            except Exception as e:
                LOGGER.exception(e)
                LOGGER.error(f"Insert failed: {e}")
            sr.id = str(dbq.LastInsertId())

            entry = (
                patient_id,
                patient_mrn,
                patient_name,
                patient_birthdate,
                patient_gender,
            )
            try:
                dbq.Insert(dbq.INSERT_PATIENT, entry)
            except Exception as e:
                LOGGER.exception(e)
                LOGGER.error(f"Insert failed: {e}")

            # add study_iuid to resource identifier
            fhir_study_iuid = {
                "use": "official",
                "system" : "urn:dicom:uid",
                "value" : f"urn:oid:{study_iuid}"
            }
            sr.identifier.append(fhir_study_iuid)

            self._set_response()
            self.wfile.write(sr.json().encode('utf-8'))
        else:
            self._set_response(400)
            self.wfile.write(json.dumps(response_invalid_resource).encode('utf-8'))
            return


def start_server(port):
    server_address = ('', port)
    httpd = HTTPServer(server_address, HTTPHandler)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        # User interrupt the program with ctrl+c
        exit()
