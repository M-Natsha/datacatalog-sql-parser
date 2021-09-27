# TODO: Write code to ingest lineage into GCP
import logging
import uuid
from google.cloud.lineage_v1.types import Process

from google.cloud.lineage_v1.services.lineage.transports.grpc_asyncio import \
    LineageGrpcTransport
from google.cloud.lineage_v1.types.lineage import \
    CreateLineageEventRequest, CreateProcessRequest, CreateRunRequest, \
    LineageEvent, Run
from google.protobuf.timestamp_pb2 import Timestamp


class IngestLineage():

    def __init__(self, project_id, location_id):
        self.project_id = project_id,
        self.location_id = location_id
        self.project_name = f"projects/{project_id}/locations/{location_id}"

    def create_process(self, transport):
        processId = "test-1"
        process = Process({
            "name": self.project_name + f"/processes/{processId}"
        })

        creatingProcess = CreateProcessRequest({
            "parent": self.project_name,
            "process_id":
            processId,  # TODO: generate a name for the process_id
            "process": process,
            "request_id": str(uuid.uuid4())
        })

        x = transport.create_process(creatingProcess)
        print(x)
        logging.info("Process created")

    def create_run(self, transport, process):
        run_id = "test-1"
        run = Run({
            "name": process.name + f"/runs/{run_id}",
            "start_time": Timestamp(),
            "end_time": Timestamp(),
            "state": Run.State.SUCCEEDED
        })

        creatingRun = CreateRunRequest({
            "parent": process.name,
            "run_id": run_id,  # TODO: generate a name for the process_id
            "run": run,
            "request_id": str(uuid.uuid4())
        })

        x = transport.create_run(creatingRun)
        print(x)
        logging.info("Run created")

    def create_lineage_event(self, transport, run):
        lineage_event = "lineage_event"
        lineageEvent = LineageEvent({
            "name":
            self.project_name + f"/lineageEvents/{lineage_event}",
            "sources": [],
            "targets": [],
            "event_time":
            Timestamp()
        })

        creatingLineageRequest = CreateLineageEventRequest({
            "parent":
            run.name,
            "lineage_event":
            lineageEvent,
            "request_id":
            str(uuid.uuid4())
        })

        x = transport.create_lineage_event(creatingLineageRequest)

        print(x)
        logging.info("Lineage created")

    def ingest(self, lineage):
        transport = LineageGrpcTransport()

        print("creating a process")
        process = self.create_process(transport)

        print("Creating a run")
        run = self.create_run(transport, process)

        print("creating lineage event")
        self.create_lineage_event(transport, run)
