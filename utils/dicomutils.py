import hashlib
import hmac
import json


def make_association_id(event) -> str:
    """Generate a unique association ID from event data."""
    return f"{event.assoc.name}#{event.assoc.native_id}"


def make_hash(study_id: str) -> str:
    """Generate a HMAC-SHA256 hash for the provided study ID."""
    key = "e179317a-62b0-4996-8999-e91aabcd"
    byte_key = key.encode('UTF-8')
    message = study_id.encode()
    return hmac.new(byte_key, message, hashlib.sha256).hexdigest()


class DcmModel:
    """Represents a DICOM model with attributes extracted from a JSON string."""

    def __init__(self, json_str: str):
        self.Str = json_str
        data = json.loads(json_str)

        # Extracting relevant fields from the JSON
        self.RetrieveURL = data["00081190"]["Value"][0]
        self.StudyInstanceUID = self._extract_study_instance_uid()
        self.InstanceURL = data["00081199"]["Value"][0]["00081190"]["Value"][0]
        self.ReferencedSOPSequence = ""
        self.ReferencedSOPClassUID = ""
        self.ReferencedStudyInstanceUID = ""
        self.WarningDetail = ""
        self.Status = ""

    def _extract_study_instance_uid(self) -> str:
        """Helper method to extract StudyInstanceUID from the RetrieveURL."""
        return self.RetrieveURL.split("/")[-1]
