#!/usr/bin/env python3

# Copyright (c) 2023, Pacific Biosciences of California, Inc.
##
# All rights reserved.
##
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
# * Redistributions of source code must retain the above copyright
# notice, this list of conditions and the following disclaimer.
# * Redistributions in binary form must reproduce the above copyright
# notice, this list of conditions and the following disclaimer in the
# documentation and/or other materials provided with the distribution.
# * Neither the name of Pacific Biosciences nor the names of its
# contributors may be used to endorse or promote products derived from
# this software without specific prior written permission.
##
# NO EXPRESS OR IMPLIED LICENSES TO ANY PARTY'S PATENT RIGHTS ARE GRANTED BY
# THIS LICENSE.  THIS SOFTWARE IS PROVIDED BY PACIFIC BIOSCIENCES AND ITS
# CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT
# NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
# PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL PACIFIC BIOSCIENCES OR
# ITS CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
# EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
# PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
# OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
# WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
# OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
# ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

"""
SMRT Link REST API reference client implementation, for version 12.0.0
or newer.  This is written to be self-contained without any
dependencies on internal PacBio libraries, and can be copied, modified, and
redistributed without limitation (see module comments for license terms).

The SmrtLinkClient does not cover the entire API, but the code is intended to
be easily extended and translated into other languages.  For simplicity and
readability, the returned objects are weakly typed lists
and dicts containing basic Python types (str, int, float, bool, None).
The Swagger documentation in the SMRT Link GUI online help
(https://servername:8243/sl/docs/services) provides a comprehensive listing
of endpoints and data models.

Software requirements: Python >= 3.9; 'requests' module

Example module usage:

  - Creating a client object, including authentication::

    client = SmrtLinkClient.connect("localhost", user="pbuser", password="XXX")

  - Get all analysis jobs associated with a run::

    run = client.get_run(run_uuid)
    collections = client.get_run_collections(run_uuid)
    jobs = []
    for collection in collections:
        jobs.extend(client.get_dataset_jobs(collection["ccsId"]))

  - Import a HiFi dataset XML, and fetch a list of all datasets that were
    loaded (including any demultiplexed children)::

    import_job = client.create_import_dataset_job(xml_file)
    finished_job = client.poll_for_successful_job(import_job["id"])
    datasets = client.get_consensusreadsets(jobId=import_job["id"])

  - Retrieve barcoded sample metrics for a run collection and dump to CSV::

    reports = client.get_run_collection_reports(run_uuid, collection_uuid)
    for r in reports:
        if r["reportTypeId"].split(".")[-1] == "report_barcode":
            report_uuid = r["dataStoreFile"]["uuid"]
            report = client.load_datastore_report_file(report_uuid)
            bc_table = report["tables"][0]
            headers = [c["header"] for c in bc_table["columns"]]
            col_data = [c["values"] for c in bc_table["columns"]]
            table_rows = [[c[i] for c in col_data] for i in len(col_data[0])]
            write_csv(csv_file_name, headers, table_rows)

  - Retrieve loading metrics for a run collection and return as a dict::

    reports = client.get_run_collection_reports(run_uuid, collection_uuid)
    for r in reports:
        if "loading" in r["reportTypeId"]:
            report_uuid = r["dataStoreFile"]["uuid"]
            report = client.load_datastore_report_file(report_uuid)
            print({attr["id"]:attr["value"] for attr in report["attributes"]})

  - Combine a sample split across multiple cells:

    DS_TYPE = "PacBio.DataSet.ConsensusReadSet"
    datasets = client.get_consensusreadsets(bioSampleName="MySample1234")
    job = client.create_merge_datasets_job([d["uuid"] for d in datasets])
    job = client.poll_for_successful_job(job["id"])
    datastore = client.get_job_datastore(job["id"])
    merged_datasets = [f for f in datastore if f["fileTypeId"] == DS_TYPE]
    # alternately:
    merged_datasets = client.get_consensusreadsets(jobId=job["id"])

  - Find the Run QC reports associated with an analysis job::

    entry_points = client.get_job_entry_points(job_id)
    movie_names = set([])
    for entry_point in entry_points:
        if entry_point["datasetType"] == "PacBio.DataSet.ConsensusReadSet":
            dataset = client.get_consensusreadset(entry_point["datasetUUID"])
            movie_names.append(dataset["metadataContextId"])
    qc_reports = []
    for movie_name in movie_names:
        runs = client.get_runs(movieName=movie_name)
        if len(runs) == 1:
            collections = client.get_run_collection(runs[0]["unique_id"])
            for collection in collections:
                if collection["context"] == movie_name:
                    reports = client.get_collection_reports(run_id,
                        collection["unique_id"])
                    qc_reports.append(reports)

  - Poll every 10 minutes until a collection is complete, then launch a HiFi
    Mapping job using the official PacBio hg38 reference, and poll until it
    completes successfully::

    collection = client.get_run_collection(run_id, collection_id)
    while True:
        dataset = client.get_dataset_search(collection["ccsId"])
        if dataset:
            break
        else:
            time.sleep(600)
    job = client.create_analysis_job({
        "name": "My Mapping Job",
        "pipelineId": "cromwell.workflows.pb_align_ccs",
        "entryPoints": [
            {
                "entryId": "eid_ccs",
                "datasetId": collection["ccsId"],
                "fileTypeId": "PacBio.DataSet.ConsensusReadSet"
            },
            {
                "entryId": "eid_ref_dataset",
                "datasetId": "ba3866bf-2aba-7c99-0570-0d6709174e4a",
                "fileTypeId": "PacBio.DataSet.ReferenceSet"
            }
        ],
        "taskOptions": [],
        "workflowOptions": []
    })
    job = client.poll_for_successful_job(job["id"])
"""

from abc import ABC, abstractmethod
import urllib.parse
import argparse
import logging
import json
import time
import os
import sys

# This is the only non-standard dependency
import requests

__all__ = [
    "SmrtLinkClient",
    "add_smrtlink_server_args",
    "get_smrtlink_client_from_args",
]

log = logging.getLogger(__name__)


class Constants:
    API_PORT = 8243
    H_CT_AUTH = "application/x-www-form-urlencoded"
    H_CT_JSON = "application/json"
    # SSL is good and we should not disable it by default
    DEFAULT_VERIFY = True


def refresh_on_401(f):
    """
    Method decorator to trigger a token refresh when an HTTP 401 error is
    received.
    """

    def wrapper(self, *args, **kwds):
        try:
            return f(self, *args, **kwds)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                self.refresh()
                return f(self, *args, **kwds)
            else:
                raise
    return wrapper


def _disable_insecure_warning():
    """
    Workaround to silence SSL warnings when the invoker has explicitly
    requested insecure mode.
    """
    from urllib3.exceptions import ProtocolError, InsecureRequestWarning  # pylint: disable=import-error
    # To disable the ssl cert check warning
    import urllib3
    import warnings
    warnings.warn("You are running with some SSL security features disabled (verify=False).  Please note that this is considered risky and exposes you to remote attacks that can steal your authorization credentials or sensitive data.", InsecureRequestWarning)
    urllib3.disable_warnings(
        InsecureRequestWarning)  # pylint: disable=no-member


class RESTClient(ABC):
    """
    Base class for interacting with any REST API that communicates primarily
    in JSON.
    """
    PROTOCOL = "http"

    def __init__(self, host, port, verify=Constants.DEFAULT_VERIFY):
        self.host = host
        self.port = port
        self._verify = verify

    @abstractmethod
    def refresh(self):
        ...

    @property
    def headers(self):
        return {"Content-Type": Constants.H_CT_JSON}

    @property
    def base_url(self):
        return f"{self.PROTOCOL}://{self.host}:{self.port}"

    def to_url(self, path):
        """Convert an API method path to the full server URL"""
        return f"{self.base_url}{path}"

    def _get_headers(self, other_headers={}):
        headers = dict(self.headers)
        if other_headers:
            headers.update(other_headers)
        return headers

    @refresh_on_401
    def _http_get(self, path, params=None, headers={}):
        if isinstance(params, dict):
            if len(params) == 0:
                params = None
            else:
                # get rid of queryParam=None elements
                params = {k: v for k, v in params.items() if v is not None}
        url = self.to_url(path)
        log.info(f"Method: GET {path}")
        log.debug(f"Full URL: {url}")
        response = requests.get(url,
                                params=params,
                                headers=self._get_headers(headers),
                                verify=self._verify)
        log.debug(response)
        response.raise_for_status()
        return response

    @refresh_on_401
    def _http_post(self, path, data, headers={}):
        url = self.to_url(path)
        log.info(f"Method: POST {path} {data}")
        log.debug(f"Full URL: {url}")
        response = requests.post(url,
                                 data=json.dumps(data),
                                 headers=self._get_headers(headers),
                                 verify=self._verify)
        log.debug(response)
        response.raise_for_status()
        return response

    @refresh_on_401
    def _http_put(self, path, data, headers={}):
        url = self.to_url(path)
        log.info(f"Method: PUT {path} {data}")
        log.debug(f"Full URL: {url}")
        response = requests.put(url,
                                data=json.dumps(data),
                                headers=self._get_headers(headers),
                                verify=self._verify)
        log.debug(response)
        response.raise_for_status()
        return response

    @refresh_on_401
    def _http_delete(self, path, headers={}):
        url = self.to_url(path)
        log.info(f"Method: DELETE {path}")
        log.debug(f"Full URL: {url}")
        response = requests.delete(url,
                                   headers=self._get_headers(headers),
                                   verify=self._verify)
        log.debug(response)
        response.raise_for_status()
        return response

    @refresh_on_401
    def _http_options(self, path, headers={}):
        url = self.to_url(path)
        log.info(f"Method: OPTIONS {url}")
        log.debug(f"Full URL: {url}")
        response = requests.options(url,
                                    headers=self._get_headers(headers),
                                    verify=self._verify)
        log.debug(response)
        response.raise_for_status()
        return response

    def get(self, path, params=None, headers={}):
        """Generic JSON GET method handler"""
        return self._http_get(path, params, headers).json()

    def post(self, path, data, headers={}):
        """Generic JSON POST method handler"""
        return self._http_post(path, data, headers).json()

    def put(self, path, data, headers={}):
        """Generic JSON PUT method handler"""
        return self._http_put(path, data, headers).json()

    def delete(self, path, headers={}):
        """Generic JSON DELETE method handler"""
        return self._http_delete(path, headers).json()

    def options(self, path, headers={}):
        """
        OPTIONS handler, used only for getting CORS settings in ReactJS.
        Since the response body is empty, the return value is the response
        headers (as a dict).
        """
        return self._http_options(path, headers).headers()

    def execute_call(self, method, path, data, headers):
        """Execute any supported JSON-returning HTTP call by name"""
        if method == "GET":
            return self.get(path, headers=headers)
        elif method == "POST":
            return self.post(path, data=data, headers=headers)
        elif method == "PUT":
            return self.put(path, data=data, headers=headers)
        elif method == "DELETE":
            return self.get(path, headers=headers)
        elif method == "OPTIONS":
            return self.options(path, headers=headers)
        else:
            raise ValueError(f"Method '{method}' not supported")


class AuthenticatedClient(RESTClient):
    """
    Base class for REST clients that require authorization via the Oauth2
    token interface.
    """

    def __init__(self, host, port, username, password,
                 verify=Constants.DEFAULT_VERIFY):
        super(AuthenticatedClient, self).__init__(host, port, verify)
        self._user = username
        self._oauth2 = self.get_authorization_token(username, password)

    @property
    def auth_token(self):
        return self._oauth2["access_token"]

    @property
    def refresh_token(self):
        return self._oauth2["refresh_token"]

    @property
    def headers(self):
        """Dict of default HTTP headers for all endpoints"""
        return {
            "Content-Type": Constants.H_CT_JSON,
            "Authorization": f"Bearer {self.auth_token}"
        }

    @abstractmethod
    def get_authorization_token(self, username, password):
        ...


class SmrtLinkClient(AuthenticatedClient):
    """
    Class for executing methods on the secure (authenticated) SMRT Link REST
    API, via API gateway
    """
    PROTOCOL = "https"
    JOBS_PATH = "/smrt-link/job-manager/jobs"

    def __init__(self, *args, **kwds):
        if not kwds.get("verify", Constants.DEFAULT_VERIFY):
            _disable_insecure_warning()
        super(SmrtLinkClient, self).__init__(*args, **kwds)

    @staticmethod
    def connect(host, username, password, verify=Constants.DEFAULT_VERIFY):
        """
        Convenience method for instantiating a client using the default
        API port 8243
        """
        return SmrtLinkClient(host=host,
                              port=Constants.API_PORT,
                              username=username,
                              password=password,
                              verify=verify)

    @property
    def headers(self):
        return {
            "Content-Type": Constants.H_CT_JSON,
            "Authorization": f"Bearer {self.auth_token}",
            "X-User-ID": self._user
        }

    def to_url(self, path):
        return f"{self.base_url}/SMRTLink/2.0.0{path}"

    def get_authorization_token(self, username, password):
        """
        Request an Oauth2 authorization token from the SMRT Link API
        server, which is actually a proxy to Keycloak.  This token will
        enable access to all API methods that are allowed for the role of
        the authorized user.
        """
        auth_d = dict(username=username,
                      password=password,
                      grant_type="password")
        resp = requests.post(f"{self.base_url}/token",
                             data=auth_d,
                             headers={"Content-Type": Constants.H_CT_AUTH},
                             verify=self._verify)
        resp.raise_for_status()
        t = resp.json()
        log.info("Access token: {}...".format(t["access_token"][0:40]))
        log.debug("Access token: {}".format(t["access_token"]))
        return t

    def refresh(self):
        """
        Attempt to refresh the authorization token, using the refresh token
        obtained in a previous request.
        """
        log.info("Requesting new access token using refresh token")
        auth_d = dict(grant_type="refresh_token",
                      refresh_token=self.refresh_token)
        resp = requests.post(self.to_url("/token"),
                             data=auth_d,
                             headers={"Content-Type": Constants.H_CT_AUTH},
                             verify=self._verify)
        resp.raise_for_status()
        t = resp.json()
        log.info("Access token: {}...".format(t["access_token"][0:40]))
        log.debug("Access token: {}".format(t["access_token"]))
        self._oauth2 = t
        return t

    # -----------------------------------------------------------------
    # ADMINISTRATION
    def get_status(self):
        """Get status of server backend"""
        return self.get("/status")

    def set_system_config_param(self, key, value):
        """
        Set a SMRT Link system configuration parameter, such as the
        instrument user password
        """
        body = dict(key=key, value=value)
        return self.post("/smrt-link/admin-config", body)

    def get_swagger_api(self):
        """Fetch the Swagger API definition for the server"""
        return self.get("/smrt-link/swagger")

    def get_software_manifests(self):
        """Get a list of software components and versions"""
        return self.get("/smrt-link/manifests")

    def get_software_manifest(self, component_id):
        """Retrieve version information for a specific component"""
        return self.get(f"/smrt-link/manifests/{component_id}")

    def get_instrument_connections(self):
        """
        Get a list of Revio instrument connections.
        """
        return self.get("/smrt-link/instrument-config/connections")

    def create_instrument_connection(self,
                                     host,
                                     secret_key,
                                     name="Revio Instrument"):
        """
        Create a new Revio instrument connection in SMRT Link.  The server
        will automatically perform a full handshake and configure ICS.
        """
        body = {
            "host": host,
            "serial": "unknown",  # NOTE the server gets this from ICS
            "name": name,
            "credentials": secret_key
        }
        return self.post("/smrt-link/instrument-config/connections", body)

    def update_instrument_connection(self, instrument_id, update_d):
        """Update an existing Revio instrument connection"""
        path = f"/smrt-link/instrument-config/connections/{instrument_id}"
        return self.put(path, update_d)

    def connect_instrument(self, instrument_id):
        """Activate an existing Revio instrument connection"""
        return self.update_instrument_connection(instrument_id,
                                                 dict(isConnected=True))

    def delete_instrument_connection(self, id_name_or_serial):
        """Delete an instrument connection record"""
        return self.delete(f"/smrt-link/instrument-config/connections/{id_name_or_serial}")

    def get_instrument_states(self):
        """
        Return a list of instrument states, complex objects that include
        configuration details and run progress.  Connected instruments should
        send state updates once per minute.
        """
        return self.get("/smrt-link/instruments")

    def get_instrument_state(self, serial):
        """
        Return the last recorded state for a specific instrument by serial
        number.
        """
        return self.get(f"/smrt-link/instruments/{serial}")

    def delete_instrument_state(self, serial):
        """
        Remove an instrument from the Instruments status page in SMRT Link (but
        not from the Instrument Settings page)
        """
        return self.delete(f"/smrt-link/instruments/{serial}")

    # -----------------------------------------------------------------
    # RUNS
    def get_runs(self, **search_params):
        """
        Get a list of all PacBio instrument runs, with optional search
        parameters.

        Partial list of supported search parameters:
            name (partial matches supported)
            reserved (boolean, true means selected on instrument)
            instrumentType (Revio, Sequel2e, or Sequel2)
            chipType (8mChip or 25mChip)
            collectionUuid (retrieve the run for a specific collection)
            movieName
        """
        return self.get("/smrt-link/runs", dict(search_params))

    def get_run(self, run_id):
        """Retrieve a PacBio instrument run description by UUID"""
        return self.get(f"/smrt-link/runs/{run_id}")

    def get_run_xml(self, run_id):
        """
        Retrieve the XML data model for a PacBio instrument run
        """
        return self._http_get(f"/smrt-link/runs/{run_id}/datamodel").text

    def get_run_collections(self, run_id):
        """Retrieve a list of collections/samples for a run"""
        return self.get(f"/smrt-link/runs/{run_id}/collections")

    def get_run_collection(self, run_id, collection_id):
        """Retrieve metadata for a single collection in a run"""
        return self.get(f"/smrt-link/runs/{run_id}/collections/{collection_id}")

    def get_run_from_collection_id(self, collection_id):
        """
        Convenience method wrapping get_runs(), for retrieving a run based
        on collection UUID alone.  Returns None if no matching run is found.
        """
        runs = self.get_runs(collectionUuid=collection_id)
        return None if len(runs) == 0 else runs[0]

    def get_run_collection_reports(self, run_id, collection_id):
        """
        Get all reports associated with a run collection
        Introduced in SMRT Link 13.0
        """
        return self.get(f"/smrt-link/runs/{run_id}/collections/{collection_id}/reports")

    def get_run_collection_barcodes(self, run_id, collection_id):
        """Get a list of barcoded samples associated with a run collection"""
        return self.get(f"/smrt-link/runs/{run_id}/collections/{collection_id}/barcodes")

    def get_run_collection_hifi_reads(self, run_id, collection_id):
        """
        Retrieve the HiFi dataset that is the primary output of a PacBio
        instrument run.
        """
        collection = self.get_run_collection(run_id, collection_id)
        return self.get_consensusreadset(collection["ccsId"])

    def get_run_collection_hifi_reads_barcoded_datasets(self,
                                                        run_id,
                                                        collection_id,
                                                        barcode_name=None,
                                                        biosample_name=None):
        """
        Retrieve the demultiplexed "child" datasets for a PacBio instrument
        run, optionally filtering by barcode name (e.g. 'bc2001--bc2001') or
        biosample name.
        """
        collection = self.get_run_collection(run_id, collection_id)
        return self.get_barcoded_child_datasets(collection["ccsId"],
                                                barcode_name=barcode_name,
                                                biosample_name=biosample_name)

    def get_run_reports(self, run_id):
        """
        Get all collection-level reports associated with a run.
        Introduced in SMRT Link 13.0
        """
        return self.get(f"/smrt-link/runs/{run_id}/reports")

    def get_run_design(self, run_id):
        """Return the run design JSON object used by the SMRT Link GUI"""
        return self.get(f"/smrt-link/run-design/{run_id}")

    def import_run_design_csv(self, csv_file):
        """
        Import a Run CSV definition and return the run-design data model.
        This is the officially supported interface for creating a Run Design
        programatically.
        """
        csv_d = {"content": open(csv_file, "rt").read()}
        return self.post("/smrt-link/import-run-design", csv_d)

    def delete_run(self, run_id):
        """Delete a PacBio run description by UUID"""
        return self.delete(f"/smrt-link/runs/{run_id}")

    def import_run_xml(self, xml_file):
        """
        Post a Run XML directly to the API.  This is not officially supported
        as an integration mechanism, but is useful for transferring Run QC
        results between servers.
        """
        return self.post("/smrt-link/runs", {"dataModel": open(xml_file).read()})

    def update_run_xml(self, xml_file, run_id, is_reserved=None):
        """
        Update a Run data model XML.  This endpoint is what the Revio and Sequel
        II/IIe instruments use to communicate run progress, and to mark a run
        as "reserved" by a particular instrument.  It can be used as a workaround
        for updating the status of incomplete runs after manual XML edits.
        """
        opts_d = {"dataModel": open(xml_file).read()}
        if is_reserved is not None:
            opts_d["reserved"] = is_reserved
        return self.post(f"/smrt-link/runs/{run_id}", opts_d)

    # -----------------------------------------------------------------
    # CHEMISTRY BUNDLE
    def get_active_bundle_metadata(self, bundle_type):
        """
        Return the metadata for the current version of a bundle
        """
        return self.get(f"/smrt-link/bundles/{bundle_type}/active")

    def get_chemistry_bundle_metadata(self):
        """
        Return the metadata for the current version of the chemistry-pb
        bundle used by Sample Setup, Run Design, and the instrument software
        """
        return self.get_active_bundle_metadata("chemistry-pb")

    def get_active_bundle_file(self, bundle_type, relative_path):
        """
        Retrieve the contents of a file in a bundle
        """
        relpath = urllib.parse.quote(relative_path)
        path = f"/smrt-link/bundles/{bundle_type}/active/files/{relpath}"
        return self._http_get(path).text

    def get_chemistry_bundle_file(self, relative_path):
        """
        Retrieve the contents of a file in the chemistry bundle.  The list
        of consumables is in 'definitions/PacBioAutomationConstraints.xml';
        settings for Run Design applications are in 'RunDesignDefaults.json'.
        """
        return self.get_active_bundle_file("chemistry-pb", relative_path)

    # -----------------------------------------------------------------
    # MISC SERVICES
    def download_datastore_file(self, file_uuid):
        path = f"/smrt-link/datastore-files/{file_uuid}/download"
        return self._http_get(path).content

    def load_datastore_report_file(self, file_uuid):
        """
        Convenience wrapper for downloading a datastore report.json file in
        memory and and converting to Python objects.
        """
        return json.loads(self.download_datastore_file(file_uuid))

    def download_file_resource(self, file_uuid, resource_name):
        """
        Retrieve another file (usually a PNG file) referenced by a datastore
        file (usually a Report) and return the raw data
        """
        path = f"/smrt-link/datastore-files/{file_uuid}/resources"
        return self._http_get(path, params={"relpath": resource_name}).content

    # -----------------------------------------------------------------
    # DATASETS
    def _get_datasets_by_type(self, dataset_type, **query_args):
        return self.get(f"/smrt-link/datasets/{dataset_type}",
                        params=dict(query_args))

    def _get_dataset_by_type_and_id(self, dataset_type, dataset_id):
        return self.get(f"/smrt-link/datasets/{dataset_type}/{dataset_id}")

    def _get_dataset_resources_by_type_and_id(self, dataset_type, dataset_id, resource_type):
        return self.get(f"/smrt-link/datasets/{dataset_type}/{dataset_id}/{resource_type}")

    def get_consensusreadsets(self, **query_args):
        """
        Retrieve a list of HiFi datasets, with optional search parameters.
        Partial list of supported search terms:
            name
            bioSampleName
            wellSampleName
            metadataContextId (movie name)
        String searches are always case-insensitive.
        Most of the non-timestamp string fields in the data model are
        searchable with partial strings by adding the prefix 'like:' to the
        search term, thus:
            client.get_consensusreadsets(bioSampleName="like:HG002")
        The refixes 'not:' (inequality), 'unlike:', 'start:', and 'end:' are
        also recognized.  For numerical fields, 'not:', 'lt:', 'lte:', 'gt:',
        'gte:' are supported, plus 'range:{start},{end}'.
        """
        return self._get_datasets_by_type("ccsreads", **query_args)

    def get_consensusreadsets_by_movie(self, movie_name):
        """
        Retrieve a list of HiFi datasets for a unique movie name (AKA
        'context' or 'metadataContextId'), such as 'm84001_230601_123456'
        """
        return self.get_consensusreadsets(metadataContextId=movie_name)

    def get_barcoded_child_datasets(self,
                                    parent_dataset_id,
                                    barcode_name=None,
                                    biosample_name=None):
        """Get a list of demultiplexed children (if any) of a HiFi dataset"""
        return self.get_consensusreadsets(parentUuid=parent_dataset_id,
                                          dnaBarcodeName=barcode_name,
                                          bioSampleName=biosample_name)

    def get_subreadsets(self, **query_args):
        """
        Retrieve a list of CLR/subread datasets, with optional search
        parameters. (DEPRECATED)
        """
        return self._get_datasets_by_type("subreads", **query_args)

    def get_referencesets(self, **query_args):
        """Get a list of ReferenceSet datasets"""
        return self._get_datasets_by_type("references", **query_args)

    def get_barcodesets(self, **query_args):
        """Get a list of BarcodeSet datasets (including MAS-Seq adapters and Iso-Seq primers)"""
        return self._get_datasets_by_type("barcodes", **query_args)

    def get_consensusreadset(self, dataset_id):
        """Get a HiFi dataset by UUID or integer ID"""
        return self._get_dataset_by_type_and_id("ccsreads", dataset_id)

    def get_subreadset(self, dataset_id):
        """Get a CLR (subread) dataset by UUID or integer ID (DEPRECATED)"""
        return self._get_dataset_by_type_and_id("subreads", dataset_id)

    def get_referenceset(self, dataset_id):
        """GET /smrt-link/datasets/references/{dataset_id}"""
        return self._get_dataset_by_type_and_id("references", dataset_id)

    def get_barcodeset(self, dataset_id):
        """GET /smrt-link/datasets/barcodes/{dataset_id}"""
        return self._get_dataset_by_type_and_id("barcodes", dataset_id)

    def get_consensusreadset_reports(self, dataset_id):
        """Get a list of reports associated with a HiFi dataset"""
        return self._get_dataset_resources_by_type_and_id("ccsreads", dataset_id, "reports")

    def get_barcodeset_contents(self, dataset_id):
        """
        Retrieve the entire contents of a BarcodeSet dataset, as a
        raw FASTA string (not JSON!)
        """
        path = f"/smrt-link/datasets/barcodes/{dataset_id}/contents"
        return self._http_get(path).text

    def get_barcodeset_record_names(self, dataset_id):
        """Retrieve a list of barcode/primer/adapter names in a BarcodeSet"""
        return self._get_dataset_resources_by_type_and_id("barcodes", dataset_id, "record-names")

    def get_dataset_metadata(self, dataset_id):
        """Retrieve a type-independent dataset metadata object"""
        return self._get_dataset_by_type_and_id("meta", dataset_id)

    def get_dataset_jobs(self, dataset_id):
        """Get a list of analysis jobs that used the specified dataset as input"""
        return self._get_dataset_resources_by_type_and_id("meta", dataset_id, "jobs")

    def get_dataset_search(self, dataset_id):
        """
        Retrieve a single dataset if it is present in the database, or None
        if it is missing, without triggering an HTTP 404 error in the latter
        case.
        """
        result_d = self.get(f"/smrt-link/datasets/search/{dataset_id}")
        if not result_d:  # empty dict is the "not found" response
            return None
        return result_d

    # -----------------------------------------------------------------
    # JOBS
    def _get_jobs_by_type(self, job_type, params=None):
        return self.get(f"{self.JOBS_PATH}/{job_type}", params=params)

    def _post_job_by_type(self, job_type, opts_d):
        return self.post(f"{self.JOBS_PATH}/{job_type}", opts_d)

    def _get_job_by_type_and_id(self, job_type, job_id):
        return self.get(f"{self.JOBS_PATH}/{job_type}/{job_id}")

    def _get_job_resources_by_type_and_id(self, job_type, job_id, resource_type):
        return self.get(f"{self.JOBS_PATH}/{job_type}/{job_id}/{resource_type}")

    def _get_job_resource_by_type_and_id(self, job_type, job_id, resource_type, resource_id):
        return self.get(f"{self.JOBS_PATH}/{job_type}/{job_id}/{resource_type}/{resource_id}")

    # NOTE our API routing is deliberately very permissive due to
    # limitations of the underlying toolkits, so we often use the
    # 'analysis' endpoints for GETting individual job attributes,
    # regardless of job type
    def get_job(self, job_id):
        """
        Retrieve a job of any type by integer ID or UUID.
        """
        return self._get_job_by_type_and_id("analysis", job_id)

    def get_job_reports(self, job_id):
        """
        Get a list of reports generated by a job.  The UUIDs of these
        objects can be used to retrieve the full report content using
        get_job_report()
        """
        return self._get_job_resources_by_type_and_id("analysis", job_id,
                                                      "reports")

    def get_job_report(self, job_id, report_uuid):
        """
        Retrieve the Report data for a specific report output by a job.
        """
        return self._get_job_resource_by_type_and_id("analysis", job_id,
                                                     "reports", report_uuid)

    def download_job_report_resource(self, job_id, report_uuid, resource_name):
        """
        Retrieve a plot (usually a PNG file) referenced by a Report object,
        and return the raw image data
        """
        path = f"{self.JOBS_PATH}/analysis/{job_id}/reports/{report_uuid}/resources"
        return self._http_get(path, params={"relpath": resource_name}).content

    def get_job_datastore(self, job_id):
        """
        Get a list of all exposed output files generated by a job
        """
        return self._get_job_resources_by_type_and_id("analysis", job_id,
                                                      "datastore")

    def get_job_entry_points(self, job_id):
        """Get the dataset UUIDs used to create the job"""
        return self._get_job_resources_by_type_and_id("analysis", job_id,
                                                      "entry-points")

    def get_job_datasets(self, job_id):
        """Alias for get_job_entry_points(job_id)"""
        return self.get_job_entry_points(job_id)

    def get_job_options(self, job_id):
        """Get the options model used to create the job"""
        return self._get_job_resources_by_type_and_id("analysis", job_id,
                                                      "options")

    def download_job_datastore_file(self, job_id, file_id):
        """
        Get the raw content for a specific file in the job datastore.  Note
        that this is effectively redundant with download_datastore_file.
        """
        path = f"{self.JOBS_PATH}/analysis/{job_id}/datastore/{file_id}/download"
        return self._http_get(path).content

    def get_analysis_jobs(self, **search_params):
        """
        Get a list of standalone analysis jobs, with optional search/filter
        parameters.  This is equivalent to the "flat view" in the SMRT Analysis
        job catalog.
        """
        return self._get_jobs_by_type("analysis", search_params)

    def get_analysis_jobs_by_state(self, state):
        """
        Get a list of analysis jobs in a specified state.  Supported states:
          CREATED SUBMITTED RUNNING SUCCESSFUL FAILED TERMINATED ABORTED
        """
        return self.get_analysis_jobs(state=state)

    def get_analysis_jobs_by_parent(self, multi_job_id):
        """
        Get all children of the multi-analysis job whose ID is specified
        """
        return self.get_analysis_jobs(parentMultiJobId=multi_job_id)

    def get_smrt_analysis_nested_jobs(self, **search_params):
        """
        Get the 'nested' view of analysis jobs displayed by default in the
        SMRT Analysis jobs catalog, where children of a multi-analysis job are
        hidden at the top level
        """
        return self.get("/smrt-link/job-manager/analysis-jobs",
                        params=search_params)

    def create_analysis_job(self, opts_d):
        """
        Submit a SMRT Analysis job to be run as soon as possible.  Requires
        that all input datasets have already been imported.

        Job options schema:
            pipelineId: string such as 'cromwell.workflows.pb_align_ccs'
            name: job name string
            entryPoints: list of dataset entry points
            taskOptions: list of workflow task options
            projectId: int or null
            presetId: string or null

        Entry point model:
            entryId: pre-set identifier, can be any of 'eid_ccs',
                     'eid_barcode', 'eid_ref_dataset', 'eid_barcode_2',
                     or 'eid_subread'
            fileTypeId: dataset MetaType, from the top-level XML tag
            datasetId: dataset UniqueID (UUID)

        Task/workflow option model:
            optionId: string ID such as 'mapping_min_length'
            value: string, float, int, bool, or occasionally null
            optionTypeId: type of 'value' field
        """
        # NOTE workflowOptions is still technically required by the REST API
        # but will become optional in future releases
        if "workflowOptions" not in opts_d:
            opts_d["workflowOptions"] = []
        return self._post_job_by_type("analysis", opts_d)

    def terminate_analysis_job(self, job_id):
        """Immediately terminate a running analysis job."""
        return self.post(f"{self.JOBS_PATH}/analysis/{job_id}/terminate", {})

    def get_import_dataset_jobs(self, **search_params):
        """Get a list of import-dataset jobs"""
        return self._get_jobs_by_type("import-dataset", search_params)

    def _post_job_with_path_opt(self, job_type, opts_d_or_path, key="path"):
        opts_d = opts_d_or_path
        if isinstance(opts_d_or_path, str):
            opts_d = {key: opts_d_or_path}
        return self._post_job_by_type(job_type, opts_d)

    def create_import_dataset_job(self, opts_d_or_path):
        """
        Submit an import-dataset job, using the path to the dataset XML.

        :param opts_d_or_path: either a dictionary containing at least the
                               'path' field, or an actual path string
        """
        return self._post_job_with_path_opt("import-dataset", opts_d_or_path)

    def create_import_datasets_zip_job(self, opts_d):
        """
        Submit an import-datasets-zip job, using the path to the zipfile
        exported by another SMRT Link server.

        :param opts_d: a dictionary containing at least the 'zipFile' field
        """
        return self._post_job_by_type("import-datasets-zip", opts_d)

    def create_import_collection_job(self, opts_d_or_path):
        """
        Submit an import-collection job, using the path to the instrument file
        moviename.reports.zip.  In SMRT Link Lite this is used to populate the
        Run QC and Data Management pages, without direct access to the XML
        files.

        :param opts_d_or_path: either a dictionary containing at least the
                               'path' field, or an actual path string
        """
        return self._post_job_with_path_opt("import-collection", opts_d_or_path)

    def create_merge_datasets_job(self, opts_d_or_dataset_ids):
        """
        Submit a merge-datasets job, using two or more unique IDs for
        ConsensusReadSet datasets.

        :param opts_d_or_dataset_ids: either a dictionary containing at least
                                      the 'ids' field, or a list of integers
                                      or UUID strings
        """
        opts_d = opts_d_or_dataset_ids
        if isinstance(opts_d_or_dataset_ids, list):
            opts_d = {"ids": opts_d_or_dataset_ids}
        return self._post_job_by_type("merge-datasets", opts_d)

    def get_pipelines(self, public_only=True):
        """
        Retrieve a list of SMRT Analysis workflow interface descriptions
        """
        def _is_private(tags):
            hidden_tags = {"dev", "internal", "alpha", "obsolete"}
            return len(set(tags).intersection(hidden_tags)) > 0

        pipelines = self.get("/smrt-link/resolved-pipeline-templates")
        if public_only:
            return [p for p in pipelines if not _is_private(p["tags"])]
        else:
            return pipelines

    def get_pipeline(self, pipeline_id):
        """
        Retrieve a SMRT Analysis workflow interface description by ID,
        such as 'pb_align_ccs' or 'cromwell.workflows.pb_align_ccs'
        """
        if not pipeline_id.startswith("cromwell.workflows"):
            pipeline_id = f"cromwell.workflows.{pipeline_id}"
        return self.get(f"/smrt-link/resolved-pipeline-templates/{pipeline_id}")

    def poll_for_successful_job(self, job_id, sleep_time=10, max_time=28800):
        """
        Poll a submitted services job of any type until it completes
        successfully within the specified timeout, or raise an exception.
        This lacks the error handling that we need for heavily used servers.
        """
        t_start = time.time()
        final_states = {"SUCCESSFUL", "FAILED", "TERMINATED", "ABORTED"}
        while True:
            job = self.get_job(job_id)
            log.debug(f"Current job: {job}")
            state = job["state"]
            if state in final_states:
                log.info(f"Job {job_id} is in state {state}, polling complete")
                break
            else:
                t_current = time.time()
                if t_current - t_start > max_time:
                    raise RuntimeError(
                        f"Polling time ({max_time}s) exceeded, aborting")
                log.debug(f"Sleeping {sleep_time}s until next status check")
                time.sleep(sleep_time)
        if state != "SUCCESSFUL":
            raise RuntimeError(f"Job {job_id} exited with state {state}")
        return job

    # -----------------------------------------------------------------
    # OTHER
    def upload_file(self, file_path):
        files_d = {
            "upload_file": open(file_path, "rb")
        }
        url = self.to_url("/smrt-link/uploader")
        log.info(f"Method: POST /smrt-link/uploader {files_d}")
        log.debug(f"Full URL: {url}")
        # XXX the Content-Type header is added automatically by the requests
        # library - sending application/json results in a 404 response from
        # the akka-http API backend
        headers = {
            "Authorization": f"Bearer {self.auth_token}"
        }
        response = requests.post(url,
                                 files=files_d,
                                 headers=headers,
                                 verify=self._verify)
        log.debug(response)
        response.raise_for_status()
        return response.json()


########################################################################
# COMMAND LINE TOOLING
#
def get_smrtlink_client_from_args(args):
    """
    Instantiate a client using the argparse object, when defined with the
    add_smrtlink_server_args function
    """
    return SmrtLinkClient(
        host=args.host,
        port=args.port,
        username=args.user,
        password=args.password,
        verify=not args.insecure)


def add_smrtlink_server_args(p):
    """
    Add argparse arguments for connecting to a SMRT Link REST API, for
    developers who want to use this client in other CLI programs
    """
    DEFAULT_HOST = os.environ.get("PB_SERVICE_HOST", "localhost")
    DEFAULT_PORT = int(os.environ.get("PB_SERVICE_PORT", Constants.API_PORT))
    DEFAULT_USER = os.environ.get("PB_SERVICE_AUTH_USER", None)
    DEFAULT_PASSWORD = os.environ.get("PB_SERVICE_AUTH_PASSWORD", None)
    p.add_argument("--host",
                   action="store",
                   default=DEFAULT_HOST,
                   help="SL Server Hostname")
    p.add_argument("--port",
                   action="store",
                   type=int,
                   default=DEFAULT_PORT,
                   help="SL Server Port")
    p.add_argument("--user",
                   action="store",
                   default=DEFAULT_USER,
                   help="SL Server User Name")
    p.add_argument("--password",
                   action="store",
                   default=DEFAULT_PASSWORD,
                   help="SL Server Password")
    p.add_argument("-k", "--insecure",
                   action="store_true",
                   default=not Constants.DEFAULT_VERIFY,
                   help="Run in insecure mode without validating the SSL certificate")
    return p


def _main(argv=sys.argv):
    """
    Command line utility for generic method calls returning JSON:

      $ smrtlink_client.py GET /smrt-link/runs \\
          --host smrtlink.lab.company.com \\
          --user pbuser --password XXX
      [
        {
          "uniqueId": "2707e6c2-8bc8-4d84-98b3-5f112e7d77ff",
          "name": "Run_2023_06_01_84001",
          ...
        },
        ...
      ]
    """

    def _validate_api_path(s):
        if not s.startswith("/"):
            raise ValueError(f"Invalid URL path {s}")
        elif s.startswith("/SMRTLink"):
            raise ValueError(
                f"Please use the base API path, without /SMRTLink/1.0.0")
        return s

    parser = argparse.ArgumentParser(_main.__doc__)
    add_smrtlink_server_args(parser)
    parser.add_argument("method",
                        choices=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
                        help="HTTP method (GET, POST, PUT, DELETE, OPTIONS)")
    parser.add_argument("path",
                        type=_validate_api_path,
                        help="API method path, e.g. /smrt-link/runs")
    parser.add_argument("-d", "--data",
                        type=json.loads,
                        default={},
                        help="JSON request body (POST/PUT only)")
    parser.add_argument("-H", "--header",
                        dest="headers",
                        action="append",
                        default=[],
                        help="Optional HTTP header (multiple allowed)")
    parser.add_argument("-v", "--verbose",
                        action="store_true",
                        default=False,
                        help="Verbose mode logging (INFO)")
    parser.add_argument("--debug",
                        action="store_true",
                        default=False,
                        help="Debug mode logging (DEBUG)")
    parser.add_argument("--quiet",
                        action="store_true",
                        default=False,
                        help="Quiet mode logging (ERROR)")
    args = parser.parse_args(argv[1:])
    log_level = logging.WARN
    if args.debug:
        log_level = logging.DEBUG
    elif args.verbose:
        log_level = logging.INFO
    elif args.quiet:
        log_level = logging.ERROR
    logging.basicConfig(level=log_level,
                        stream=sys.stdout,
                        format="[%(levelname)s] %(asctime)-15sZ %(message)s")
    if args.insecure:
        _disable_insecure_warning()
    client = get_smrtlink_client_from_args(args)
    headers_d = dict([(s.strip() for s in h.split(":")) for h in args.headers])
    response = client.execute_call(args.method,
                                   args.path,
                                   args.data,
                                   headers_d)
    print(json.dumps(response, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(_main(sys.argv))
