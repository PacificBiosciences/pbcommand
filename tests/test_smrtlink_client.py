"""
Unit test for SmrtLinkClient class
"""

import xml.dom.minidom
import uuid
import json
import os

import pytest

from pbcommand.services.smrtlink_client import SmrtLinkClient

TEST_HOST = os.environ.get("PB_SERVICE_HOST", None)
TEST_USER = os.environ.get("PB_SERVICE_AUTH_USER", None)
TEST_PASSWORD = os.environ.get("PB_SERVICE_AUTH_PASSWORD", None)

@pytest.mark.skipif(TEST_HOST is None, reason="PB_SERVICE_HOST undefined")
def test_smrtlink_client_get():
    assert not None in [TEST_USER, TEST_PASSWORD]
    RUN_ID = "0f99fea6-5916-4142-9b82-220a7bb04d13"
    RUN_CTX = "r84026_20230414_212018"
    COLLECTION_ID = "69b09865-5c23-4717-92ad-75968a43f443"
    MOVIE = "m84026_230415_224020_s3"
    REPORT_ID = "e4363794-b72b-40bf-b881-180bf733416a"
    CCS_ID = "3acce2c3-d904-4d08-aba0-2628d0dcccbf"
    BARCODE_ID = "43f950a9-8bde-3855-6b25-c13368069745"
    PARTS_FILE = "definitions/PacBioAutomationConstraints.xml"
    WF_MOCK = "cromwell.workflows.dev_mock_analysis"
    WF_MAP = "cromwell.workflows.pb_align_ccs"

    client = SmrtLinkClient.connect(host=TEST_HOST,
                                    username=TEST_USER,
                                    password=TEST_PASSWORD)

    assert client.get("/status")["apiVersion"] == "2.0.0"
    assert client.get_status()["apiVersion"] == "2.0.0"
    assert len(client.get_swagger_api()["paths"]) > 100
    assert len(client.get_software_manifests()) > 200
    assert client.get_software_manifest("smrttools-pbmm2")["name"] == "pbmm2"
    assert len(client.get_instrument_connections()) > 0
    assert len(client.get_runs()) > 0
    run = client.get_run(RUN_ID)
    assert run["context"] == RUN_CTX
    run_xml = client.get_run_xml(RUN_ID)
    rundm = xml.dom.minidom.parseString(run_xml)
    assert len(rundm.getElementsByTagName("pbmeta:CollectionMetadata")) == 1
    assert len(client.get_run_collections(RUN_ID)) == 1
    collection = client.get_run_collection(RUN_ID, COLLECTION_ID)
    assert collection["context"] == MOVIE
    run2 = client.get_run_from_collection_id(COLLECTION_ID)
    assert run["context"] == RUN_CTX
    reports = client.get_run_collection_reports(RUN_ID, COLLECTION_ID)
    assert len(reports) >= 7
    assert len(client.get_run_reports(RUN_ID)) >= 7
    ds_hifi = client.get_run_collection_hifi_reads(RUN_ID, COLLECTION_ID)
    assert ds_hifi["uuid"] == collection["ccsId"]
    assert client.get_chemistry_bundle_metadata()["typeId"] == "chemistry-pb"
    parts_xml = client.get_chemistry_bundle_file(PARTS_FILE)
    parts = xml.dom.minidom.parseString(parts_xml)
    assert len(parts.getElementsByTagName("pbpn:TemplatePrepKit")) > 1
    report = json.loads(client.download_datastore_file(REPORT_ID))
    print(list(report.keys()))
    assert report["id"] == "ccs2", report
    png = client.download_file_resource(REPORT_ID, "ccs_accuracy_hist.png")
    assert len(png) > 0  # XXX not sure what to check for here
    all_hifi = client.get_consensusreadsets()
    assert len(all_hifi) > 100  # this is a safe bet on our test servers
    by_movie = client.get_consensusreadsets_by_movie(MOVIE)
    assert len(by_movie) >= 98  # there may be other derivatives too
    assert CCS_ID in {ds["uuid"] for ds in by_movie}
    assert len(client.get_barcoded_child_datasets(CCS_ID)) == 97
    assert len(client.get_barcoded_child_datasets(CCS_ID, "bc2002--bc2002")) == 1
    assert len(client.get_run_collection_hifi_reads_barcoded_datasets(
        RUN_ID, COLLECTION_ID, "bc2002--bc2002")) == 1
    ds_ccs = client.get_consensusreadset(CCS_ID)
    assert ds_ccs["uuid"] == CCS_ID
    assert len(client.get_consensusreadset_reports(CCS_ID)) >= 7
    assert client.get_dataset_search(CCS_ID)["uuid"] == CCS_ID
    assert client.get_dataset_search(uuid.uuid4()) is None
    ds_ccs_md = client.get_dataset_metadata(CCS_ID)
    assert ds_ccs_md["numRecords"] == ds_ccs["numRecords"]
    ds_bc = client.get_barcodeset(BARCODE_ID)
    assert ds_bc["numRecords"] == 112
    fasta = client.get_barcodeset_contents(BARCODE_ID)
    assert fasta.startswith(">bc2001")
    barcodes = client.get_barcodeset_record_names(BARCODE_ID)
    assert len(barcodes) == 112
    refs = client.get_referencesets()
    assert len(refs) > 1
    lambda_refs = client.get_referencesets(name="lambdaNEB")
    assert len(lambda_refs) >= 1
    assert all([ds["name"] == "lambdaNEB" for ds in lambda_refs])
    lambda_jobs = client.get_dataset_jobs(lambda_refs[0]["uuid"])
    assert len(lambda_jobs) > 0
    job = client.get_job(lambda_jobs[0]["id"])
    assert len(client.get_job_reports(lambda_jobs[0]["id"])) > 0
    import_jobs = client.get_import_dataset_jobs(state="SUCCESSFUL")
    assert len(import_jobs) > 0
    datastore = client.get_job_datastore(import_jobs[0]["id"])
    dss = [f for f in datastore if f["fileTypeId"].startswith("PacBio.DataSet")]
    assert len(dss) > 0
    success_jobs = client.get_analysis_jobs_by_state("SUCCESSFUL")
    assert len(success_jobs) > 0
    assert all([j["state"] == "SUCCESSFUL" for j in success_jobs])
    failed_jobs = client.get_analysis_jobs_by_state("FAILED")
    assert len(failed_jobs) > 0  # one of these is populated automatically
    assert all([j["state"] == "FAILED" for j in failed_jobs])
    pipelines = client.get_pipelines()
    pipeline_ids = {p["id"] for p in pipelines}
    assert WF_MAP in pipeline_ids
    assert not WF_MOCK in pipeline_ids
    all_pipelines = client.get_pipelines(public_only=False)
    all_pipeline_ids = {p["id"] for p in all_pipelines}
    assert WF_MOCK in all_pipeline_ids
    pipeline = client.get_pipeline(WF_MAP)
    assert pipeline["id"] == WF_MAP
    pipeline = client.get_pipeline(WF_MAP.split(".")[-1])
    assert pipeline["id"] == WF_MAP
    analysis_jobs = client.get_analysis_jobs()
    nested_jobs = client.get_smrt_analysis_nested_jobs()
    assert len(nested_jobs) > 0 and len(nested_jobs) < len(analysis_jobs)
    for job in nested_jobs:
        if job["isMultiJob"]:
            children = client.get_analysis_jobs_by_parent(job["id"])
            assert len(children) > 0
            assert all([j["parentMultiJobId"] == job["id"] for j in children])
