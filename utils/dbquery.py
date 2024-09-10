import logging
import sqlite3
import threading

LOGGER = logging.getLogger('pynetdicom')


class DBQuery:
    def __init__(self):
        # Initialize the database connection, cursor, and lock
        self.conn = sqlite3.connect("instance.db", uri=True, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row  # Enables access to rows by column name
        self.cursorObject = self.conn.cursor()
        self.lock = threading.Lock()

        # SQL Queries
        self.GET_LAST_INSERT_ID = "SELECT last_insert_rowid()"
        self.INSERT_SOP = "INSERT INTO dicom_obj VALUES (null,?,?,?,?,?,?,?,?,0,0)"
        self.UPDATE_ASSOC_COMPLETED = "UPDATE dicom_obj SET association_completed = 1 WHERE association_id = ?"
        self.UPDATE_INSTANCE_STATUS_SENT = "UPDATE dicom_obj SET sent_status = 1 WHERE association_id = ? AND study_iuid = ? AND series_iuid = ? AND instance_uid = ?"
        self.GET_IDS_PER_ASSOC = "SELECT DISTINCT study_iuid, accession_number FROM dicom_obj WHERE association_id = ?"
        self.GET_INSTANCES_PER_ASSOC = "SELECT study_iuid, series_iuid, instance_uid, sent_status FROM dicom_obj WHERE association_id = ? ORDER BY study_iuid, series_iuid, instance_uid"
        self.GET_INSTANCES_PER_STUDY = "SELECT series_iuid, instance_uid FROM dicom_obj WHERE association_id = ? AND study_iuid = ? ORDER BY series_iuid, instance_uid"
        self.QUERY_SOP = "SELECT * FROM dicom_obj WHERE association_id = ?"
        self.INSERT_MWL = "INSERT OR REPLACE INTO work_list VALUES (COALESCE((SELECT id FROM work_list WHERE study_iuid = ?), NULL),?,?,?,?,?,?,?,?,?,?,0)"
        self.INSERT_PATIENT = "REPLACE INTO patient VALUES (?,?,?,?,?)"
        self.GET_MWL = "SELECT a.*, b.patient_mrn, b.patient_name, b.patient_birthdate, b.patient_gender FROM work_list a LEFT JOIN patient b USING(patient_id)"

        # Create necessary tables if they don't exist
        self._create_tables()

    def _create_tables(self):
        """Creates the required tables if they don't already exist."""
        create_dicom_obj_table = """
        CREATE TABLE IF NOT EXISTS dicom_obj (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            association_id VARCHAR(256),
            scu_ae VARCHAR(32),
            scp_ae VARCHAR(32),
            accession_number VARCHAR(32),
            study_iuid VARCHAR(64),
            series_iuid VARCHAR(64),
            instance_uid VARCHAR(64),
            fs_location VARCHAR(1024),
            sent_status SMALLINT,
            association_completed SMALLINT
        );
        """
        create_patient_table = """
        CREATE TABLE IF NOT EXISTS patient (
            patient_id VARCHAR(32) PRIMARY KEY,
            patient_mrn VARCHAR(32),
            patient_name VARCHAR(256),
            patient_birthdate VARCHAR(8),
            patient_gender VARCHAR(1)
        );
        """
        create_worklist_table = """
        CREATE TABLE IF NOT EXISTS work_list (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            accession_number VARCHAR(32),
            study_iuid VARCHAR(64) UNIQUE,
            patient_id VARCHAR(32),
            modality VARCHAR(32),
            scheduled_station_ae_title VARCHAR(32),
            referring_physician_name VARCHAR(256),
            requested_procedure_id VARCHAR(64),
            requested_procedure_description VARCHAR(256),
            scheduled_procedure_step_start_date VARCHAR(8),
            scheduled_procedure_step_start_time VARCHAR(6),
            sent_status SMALLINT
        );
        """
        self.conn.execute(create_dicom_obj_table)
        self.conn.execute(create_patient_table)
        self.conn.execute(create_worklist_table)

    def _execute_query(self, query, entries=(), commit=False):
        """Executes a query with locking and optional commit."""
        try:
            self.lock.acquire(True)
            cursor = self.conn.cursor()
            cursor.execute("BEGIN;")
            cursor.execute(query, entries)
            if commit:
                cursor.execute("COMMIT;")
            return cursor
        except Exception as err:
            LOGGER.exception("Database query failed: %s", err)
            if commit:
                self.conn.rollback()
        finally:
            self.lock.release()

    def update(self, query, entries):
        """Performs an update query with thread-safe locking."""
        self._execute_query(query, entries, commit=True)

    def insert(self, query, entries):
        """Performs an insert query with thread-safe locking."""
        self._execute_query(query, entries, commit=True)

    def delete(self, query, entries):
        """Performs a delete query with thread-safe locking."""
        self._execute_query(query, entries, commit=True)

    def query(self, query, entries=()):
        """Executes a SELECT query and returns the results."""
        cursor = self._execute_query(query, entries)
        return cursor.fetchall() if cursor else None

    def last_insert_id(self):
        """Retrieves the last inserted row ID."""
        cursor = self._execute_query(self.GET_LAST_INSERT_ID)
        return cursor.fetchone()[0] if cursor else None


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
