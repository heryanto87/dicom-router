from utils.dbquery import dbquery
from pydicom.dataset import Dataset
from typing import List

class FindQuery:
    TRANSLATION = {
        "PatientID": "patient_id",
        "PatientName": "patient_name",
        "StudyInstanceUID": "study_instance_uid",
        "StudyDate": "study_date",
        "StudyTime": "study_time",
        "AccessionNumber": "accession_number",
        "StudyID": "study_id",
        "SeriesInstanceUID": "series_instance_uid",
        "Modality": "modality",
        "SeriesNumber": "series_number",
        "SOPInstanceUID": "sop_instance_uid",
        "InstanceNumber": "instance_number",
        "ScheduledProcedureStepStartDate": "scheduled_procedure_step_start_date",
        "ScheduledProcedureStepStartTime": "scheduled_procedure_step_start_time",
    }

    GET_MWL = """
    SELECT a.*, b.patient_mrn, b.patient_name, b.patient_birthdate, b.patient_gender
    FROM work_list a
    LEFT JOIN patient b USING(patient_id)
    """

    def generate_sql(self, ds: Dataset) -> str:
        """
        Generates an SQL query from a DICOM dataset by translating DICOM attributes to SQL column names.

        :param ds: DICOM dataset containing search filters
        :return: A string representing the SQL query.
        """
        filters = self._generate_filters(ds)
        sql = self.GET_MWL

        if filters:
            # Add filters to SQL query with case insensitive collation
            sql += " WHERE " + " AND ".join(filters) + " COLLATE NOCASE"

        return sql

    def _generate_filters(self, ds: Dataset) -> List[str]:
        """
        Generates a list of SQL filters based on the dataset.

        :param ds: DICOM dataset containing search filters
        :return: A list of strings representing the SQL filters.
        """
        filters = []

        for elem in ds.elements():
            if elem.value != "":
                match elem.value.__class__.__name__:
                    case "Sequence":
                        filters.extend(self._handle_sequence_element(elem))
                    case "PersonName":
                        filters.append(self._handle_person_name_element(elem))
                    case "NoneType":
                        continue
                    case _:
                        filters.append(self._handle_general_element(elem))

        return filters

    def _handle_sequence_element(self, elem) -> List[str]:
        """Handles sequence elements in the dataset."""
        filters = []
        for seq_elem in elem:
            for key, attr in self.TRANSLATION.items():
                if key in seq_elem:
                    val = getattr(seq_elem, key, "")
                    if val:
                        if key != "ScheduledProcedureStepStartDate":
                            filters.append(f"{attr} = '{val}'")
        return filters

    def _handle_person_name_element(self, elem) -> str:
        """Handles person name elements with wildcard support."""
        keyword = str(elem.value).replace("*", "%")
        keyword = f"%{keyword}%" if '%' not in keyword else keyword
        return f"{self.TRANSLATION.get(elem.keyword, '')} LIKE '{keyword}'"

    def _handle_general_element(self, elem) -> str:
        """Handles general elements."""
        try:
            return f"{self.TRANSLATION.get(elem.keyword, '')} LIKE '%{elem.value}%'"
        except KeyError:
            print(f"Element not found in translation: {elem.keyword}")
            return ""

    def _procedure_date(self, val: str) -> str:
        """Processes procedure dates with a range."""
        split_val = val.split("-")
        return next((date_val for date_val in split_val if date_val), val)
