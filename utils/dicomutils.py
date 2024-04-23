
import hashlib
import hmac
import json

def make_association_id(event):
    return event.assoc.name+"#"+str(event.assoc.native_id)


def make_hash(study_id):
    key = "e179317a-62b0-4996-8999-e91aabcd"
    byte_key = bytes(key, 'UTF-8')  # key.encode() would also work in this case
    message = study_id.encode()
    h = hmac.new(byte_key, message, hashlib.sha256).hexdigest()
    return h

class DcmModel:

  Str = ""
  StudyInstanceUID = ""
  RetrieveURL = ""
  ReferencedSOPSequence = ""
  ReferencedSOPClassUID = ""
  ReferencedStudyInstanceUID = ""
  InstanceURL = ""
  WarningDetail = ""
  Status = ""

  def __init__(self, str):
    self.Str = str
    data = json.loads(str)
    self.RetrieveURL = data["00081190"]["Value"][0]
    arr = self.RetrieveURL.split("/")
    self.StudyInstanceUID = arr[-1]
    self.InstanceURL = data["00081199"]["Value"][0]["00081190"]["Value"][0]



