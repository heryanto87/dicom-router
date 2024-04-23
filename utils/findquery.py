from utils.dbquery import dbquery

class findquery:
    TRANSLATION = {
        "PatientID": "patient_id",  # PATIENT | Unique | VM 1 | LO
        "PatientName": "patient_name",  # PATIENT | Required | VM 1 | PN
        "StudyInstanceUID": "study_instance_uid",  # STUDY | Unique | VM 1 | UI
        "StudyDate": "study_date",  # STUDY | Required | VM 1 | DA
        "StudyTime": "study_time",  # STUDY | Required | VM 1 | TM
        "AccessionNumber": "accession_number",  # STUDY | Required | VM 1 | SH
        "StudyID": "study_id",  # STUDY | Required | VM 1 | SH
        "SeriesInstanceUID": "series_instance_uid",  # SERIES | Unique | VM 1 | UI
        "Modality": "modality",  # SERIES | Required | VM 1 | CS
        "SeriesNumber": "series_number",  # SERIES | Required | VM 1 | IS
        "SOPInstanceUID": "sop_instance_uid",  # IMAGE | Unique | VM 1 | UI
        "InstanceNumber": "instance_number",  # IMAGE | Required | VM 1 | IS
        "ScheduledProcedureStepStartDate": "scheduled_procedure_step_start_date",
        "ScheduledProcedureStepStartTime": "scheduled_procedure_step_start_time",
    }
    GET_MWL="SELECT a.*, b.patient_mrn, b.patient_name, b.patient_birthdate, b.patient_gender FROM work_list a LEFT JOIN patient b USING(patient_id)"

    def GenerateSql(self, ds):
        # list of all where queries
        filters = []
        # loop search filters dataset
        for elem in ds.elements():
            # if filter has value then search attribute
            if elem.value != "":
                print("ELEM CLASS", elem.value.__class__.__name__)
                # check dataelement class
                match elem.value.__class__.__name__: 
                    case "Sequence":
                        # if sequence then loop element inside sequence
                        attr = ""
                        for v in elem:
                            print(v.__class__.__name__)
                            # loop every translation key to match every key
                            for key in self.TRANSLATION.keys():
                                # if key exists in filter
                                if key in v:
                                    attr = self.TRANSLATION[key]
                                    val = getattr(v, key)
                                    # get value and if exist append to filters
                                    if val != "":
                                        # special case for ScheduledProcedureStepStartDate
                                        # takedown reason: mindray case filter mandatory exam date w/ format ex: '20231201-20240121'
                                        if key != "ScheduledProcedureStepStartDate":
                                            # val = self.__procedureDate(val)
                                            filter = f"{attr} = '{val}'"
                                            filters.append(filter)
                    case "PersonName":
                        # detect wildcard, if exists then replace * with %, else add %string%
                        keyword = str(elem.value)
                        if '*' in keyword:
                            keyword = keyword.replace("*", "%")
                        else:
                            keyword = f"%{keyword}%"
                        filter = f"{self.TRANSLATION[elem.keyword]} like '{keyword}'"
                        filters.append(filter)
                    case "NoneType":
                        print("NO FILTER ADDED")
                    case _:
                        # else use %string%
                        try:
                            filter = f"{self.TRANSLATION[elem.keyword]} like '%{elem.value}%'"
                            filters.append(filter)
                        except:
                            print("Element not found in translation", elem.keyword)
                    
        # print(filters)
        sql = self.GET_MWL
        if len(filters) > 0:
            # collate nocase for case insensitive
            sql = sql + " WHERE " + " and ".join(filters) + " COLLATE NOCASE"

        return sql
    
    def __procedureDate(self, val):
        split_val = val.split("-")
        if '' in split_val:
            for date_val in split_val:
                if date_val != "":
                    return date_val
        return val
        