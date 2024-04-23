import logging
import sqlite3
import threading

LOGGER = logging.getLogger('pynetdicom')

class dbquery:

  conn = sqlite3.connect("instance.db", uri=True, check_same_thread = False)
  cursorObject = conn.cursor()
  lock = threading.Lock()

  GET_LAST_INSERT_ID = "SELECT last_insert_rowid()"

  INSERT_SOP="INSERT INTO dicom_obj VALUES (null,?,?,?,?,?,?,?,?,0,0)"
  UPDATE_ASSOC_COMPLETED="UPDATE dicom_obj SET association_completed = 1 WHERE association_id = ?"
  UPDATE_INSTANCE_STATUS_SENT="UPDATE dicom_obj SET sent_status = 1 WHERE association_id = ? AND study_iuid = ? AND series_iuid = ? AND instance_uid = ?"


  GET_IDS_PER_ASSOC="SELECT DISTINCT study_iuid, accession_number FROM dicom_obj WHERE association_id = ?"
  GET_INSTANCES_PER_ASSOC="SELECT study_iuid, series_iuid, instance_uid, sent_status FROM dicom_obj WHERE association_id = ? ORDER BY study_iuid, series_iuid, instance_uid"
  GET_INSTANCES_PER_STUDY="SELECT series_iuid, instance_uid FROM dicom_obj WHERE association_id = ? AND study_iuid = ? ORDER BY series_iuid, instance_uid"

  QUERY_SOP="SELECT * FROM dicom_obj WHERE association_id = ?"

  INSERT_MWL="INSERT OR REPLACE INTO work_list VALUES (COALESCE((SELECT id FROM work_list WHERE study_iuid = ?), NULL),?,?,?,?,?,?,?,?,?,?,0)"
  INSERT_PATIENT="REPLACE INTO patient VALUES (?,?,?,?,?)"
  GET_MWL="SELECT a.*, b.patient_mrn, b.patient_name, b.patient_birthdate, b.patient_gender FROM work_list a LEFT JOIN patient b USING(patient_id)"

  def __init__(self):
      createDICOMObjsTable = """
        CREATE TABLE IF NOT EXISTS dicom_obj (
            id integer PRIMARY KEY AUTOINCREMENT,
            association_id varchar(256),
            scu_ae varchar(32),
            scp_ae varchar(32),
            accession_number varchar(32),
            study_iuid varchar(64),
            series_iuid varchar(64),
            instance_uid varchar(64),
            fs_location varchar(1024),
            sent_status short,
            association_completed short
        );
      """
      self.conn.execute(createDICOMObjsTable)
      createPatientTable = """
        CREATE TABLE IF NOT EXISTS patient (
            patient_id varchar(32) PRIMARY KEY,
            patient_mrn varchar(32),
            patient_name varchar(256),
            patient_birthdate varchar(8),
            patient_gender varchar(1)
        );
      """
      self.conn.execute(createPatientTable)

      createWorkListTable = """
        CREATE TABLE IF NOT EXISTS work_list (
            id integer PRIMARY KEY AUTOINCREMENT,
            accession_number varchar(32),
            study_iuid varchar(64) UNIQUE,
            patient_id varchar(32),
            modality varchar(32),
            scheduled_station_ae_title varchar(32),
            referring_phyisician_name varchar(256),
            requested_procedure_id varchar(64),
            requested_procedure_description varchar(256),
            scheduled_procedure_step_start_date varchar(8),
            scheduled_procedure_step_start_time varchar(6),
            sent_status short
        );
      """
      self.conn.execute(createWorkListTable)

      self.cursorObject = self.conn.cursor()
      self.lock = threading.Lock()
      self.conn.row_factory = sqlite3.Row

  def Update(self, query, entries):
      try:
          self.lock.acquire(True)
          cursorObject = self.conn.cursor()
          cursorObject.execute("BEGIN;")
          #print(query+","+str(entries[0])+","+str(entries[1]))
          cursorObject.execute(query,entries)
          cursorObject.execute("COMMIT;")
      except BaseException as err:
          print(err)    
      finally:
          self.lock.release()      

  def Insert(self, query, entries):
      try:
          self.lock.acquire(True)
          cursorObject = self.conn.cursor()
          cursorObject.execute("BEGIN;")
          cursorObject.execute(query,entries)
          cursorObject.execute("COMMIT;")
          # self.conn.commit()
      except BaseException as err:
          LOGGER.exception(err)
          LOGGER.error("Insert failed")
      finally:
          self.lock.release()

  def Delete(self, query, entries):
      try:
          self.lock.acquire(True)
          cursorObject = self.conn.cursor()
          cursorObject.execute("BEGIN;")
          cursorObject.execute(query,entries)
          cursorObject.execute("COMMIT;")
          #self.conn.commit()
      except BaseException as err:
          print(err)
      finally:
          self.lock.release()              


  def Query(self, query , entries):
      try:
          self.lock.acquire(True)
          self.cursorObject.execute(query, entries)
          return self.cursorObject.fetchall()
      except BaseException as err:
          print(err)
      finally:
          self.lock.release()

  def LastInsertId(self):
      try:
          self.lock.acquire(True)
          self.cursorObject.execute("SELECT last_insert_rowid()")
          (id,) = self.cursorObject.fetchone()
      except BaseException as err:
          print(err)
      finally:
          self.lock.release()
      return id 


"""

CREATE TABLE IF NOT EXISTS patient (
    patient_id varchar(32) PRIMARY KEY,
    patient_name varchar(256),
    patient_birthdate varchar(8),
    patient_sex varchar(1)
);

CREATE TABLE IF NOT EXISTS work_list (
    id integer PRIMARY KEY AUTOINCREMENT,
    association_id varchar(256),
    scu_ae varchar(32),
    scp_ae varchar(32),
    accession_number varchar(32),
    study_iuid varchar(64),
    patient_id varchar(32),
    modality varchar(32),
    scheduled_station_ae_title varchar(32),
    referring_phyisician_name varchar(256),
    requested_procedure_id varchar(64),
    requested_procedure_description varchar(256),
    scheduled_procedure_step_start_date varchar(8),
    scheduled_procedure_step_start_time varchar(6),
    sent_status short,
    association_completed short
);

ds = Dataset()

ds.PatientName = 'Test^Test'
ds.PatientID = '123456'
ds.PatientBirthDate = '20070101'
ds.PatientSex = 'F'

ds.AccessionNumber = 'ACSN03'

ds.RequestAttributesSequence = [Dataset()]

ds.ScheduledStationAETitle = "USG09"
ds.ReferringPhysicianName = 'Bambang'
ds.RequestedProcedureID = "2123123"

req_step_seq = ds.RequestAttributesSequence
req_step_seq[0].StudyInstanceUID = study_iuid
req_step_seq[0].Modality = 'US'
req_step_seq[0].ReferencedStudySequence = []
req_step_seq[0].AccessionNumber = 'ACSN03'
req_step_seq[0].RequestedProcedureID = "1"
req_step_seq[0].RequestedProcedureDescription = 'Some procedure'
req_step_seq[0].ScheduledProcedureStepStartDate = '20231115'
req_step_seq[0].ScheduledProcedureStepStartTime = '153658'

ds.ScheduledProcedureStepSequence = [Dataset()]

sched_step_seq = ds.ScheduledProcedureStepSequence
sched_step_seq[0].StudyInstanceUID = study_iuid
sched_step_seq[0].Modality = 'US'
sched_step_seq[0].ScheduledStationAETitle = "USG09"
sched_step_seq[0].ReferencedStudySequence = []
sched_step_seq[0].AccessionNumber = 'ACSN03'
sched_step_seq[0].RequestedProcedureID = "1"
sched_step_seq[0].RequestedProcedureDescription = 'Some procedure'
sched_step_seq[0].ScheduledProcedureStepStartDate = '20231115'
sched_step_seq[0].ScheduledProcedureStepStartTime = '153658'

"""