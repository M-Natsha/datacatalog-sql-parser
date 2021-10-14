# TODO: Write code to ingest lineage into GCP
import uuid
from google.cloud.datacatalog import Process
from _typeshed import Self


class IngestLineage():
    def __init(project,location):
        Self.project = project
        Self.location = location
        
    def CreateProcess(self):
        processId = "test-1"
        process = Process()
        process.name = f"projects/{self.project}/locations/{self.location}/processes/{processId}";
        processRequest = {
            "parent" : f"projects/{self.project}/locations/{self.location}",
            "process_id" : processId, # TODO: generate a name for the process_id
            "process": process,
            "request_id": uuid.uuid4()
        }
        
        
""" attributes:
        process (google.cloud.datacatalog.lineage_v1.types.Process):
            Required. The process to be created.
    """