import datetime
from pydicom.uid import generate_uid

def new_study_iuid(organization_id):
    uuid = "2.25"
    country_id = "360"
    today_date = datetime.date.today()
    today = today_date.strftime("%Y%m%d")

    return generate_uid(prefix=f'{uuid}.{country_id}.1.{organization_id}.{today}.')