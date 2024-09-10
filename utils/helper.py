import datetime
from pydicom.uid import generate_uid

def new_study_iuid(organization_id):
    """
    Generate a new Study Instance UID (Unique Identifier) based on organization ID and current date.

    Args:
        organization_id (str): The organization ID to include in the UID.

    Returns:
        str: A new Study Instance UID.
    """
    uuid = "2.25"
    country_id = "360"
    today = datetime.date.today().strftime("%Y%m%d")

    uid_prefix = f'{uuid}.{country_id}.1.{organization_id}.{today}.'
    return generate_uid(prefix=uid_prefix)
