#!/usr/bin/python3

import argparse
from shutil import which
import aria2p
import json
import logging
import os
import random
import re
import requests
import subprocess
import sys
import tenacity
import time
import urllib3
import base64

from typing import Any, Dict, Optional


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--miner-id",
        help="Storage Provider miner ID (ie. f0123456)",
        type=str,
        default=os.environ.get("MINER_ID"),
        required=not os.environ.get("MINER_ID"),
    )
    parser.add_argument(
        "--aria2c-url",
        help="URL of the aria2c process running in daemon mode (eg. 'http://localhost:6800'). Launch the daemon with `aria2c --enable-rpc`.",
        nargs="?",
        const="http://localhost:6800",
        type=str,
        default=os.environ.get("ARIA2C_URL", "http://localhost:6800"),
        required=False,
    )
    parser.add_argument(
        "--aria2c-connections-per-server",
        help="Configures the '-x' flag in aria2c. (eg. aria2c -x8 <uri>)",
        nargs="?",
        const=10,
        type=str,
        default=os.environ.get("ARIA2C_CONNECTIONS_PER_SERVER", 10),
        required=False,
    )
    parser.add_argument(
        "--aria2c-max-concurrent-downloads",
        help="Configures the '-j' flag in aria2c. (eg. aria2c -j10)",
        nargs="?",
        const=10,
        type=str,
        default=os.environ.get("ARIA2C_MAX_CONCURRENT_DOWNLOADS", 10),
        required=False,
    )
    parser.add_argument(
        "--aria2c-download-path",
        help="The directory into which aria2c should be configured to download files before being imported to Boost. Default: /mnt/data",
        nargs="?",
        const="/mnt/data",
        type=str,
        default=os.environ.get("ARIA2C_DOWNLOAD_PATH", "/mnt/data"),
        required=False,
    )
    parser.add_argument(
        "--boost-api-info",
        help="The Boost api string normally set as the BOOST_API_INFO environment variable (eg. 'eyJhbG...aCG:/ip4/192.168.10.10/tcp/3051/http')",
        type=str,
        default=os.environ.get("BOOST_API_INFO"),
        required=not os.environ.get("BOOST_API_INFO"),
    )
    parser.add_argument(
        "--boost-graphql-port",
        help="The port number where Boost's graphql is hosted (eg. 8080)",
        nargs="?",
        const=8080,
        type=int,
        default=os.environ.get("BOOST_GRAPHQL_PORT", 8080),
        required=not os.environ.get("BOOST_GRAPHQL_PORT"),
    )
    parser.add_argument(
        "--boost-delete-after-import",
        help="Whether or not to instruct Boost to delete the downloaded data after it is imported. Equivalent of 'boostd --delete-after-import'. Default: True",
        nargs="?",
        const=True,
        type=bool,
        default=os.environ.get("BOOST_DELETE_AFTER_IMPORT", True),
        required=False,
    )
    parser.add_argument(
        "--spade-deal-timeout",
        help="The time to wait between a deal appearing in Boost and appearing in Spade before considering the deal failed (or not a Spade deal) and ignoring it. Stated in seconds, with no units. Default: 900",
        nargs="?",
        const=True,
        type=int,
        default=os.environ.get("SPADE_DEAL_TIMEOUT", 900),
        required=False,
    )
    parser.add_argument(
        "--maximum-boost-deals-in-flight",
        help="The maximum number of deals in 'Awaiting Offline Data Import' state in Boost UI. Default: 10",
        nargs="?",
        const=10,
        type=int,
        default=os.environ.get("MAXIMUM_BOOST_DEALS_IN_FLIGHT", 10),
        required=False,
    )
    parser.add_argument(
        "--maximum-pipeline-deals-in-flight",
        help="The maximum number of deals in 'Awaiting Offline Data Import' state in Boost UI. Default: 10",
        nargs="?",
        const=18,
        type=int,
        default=os.environ.get("MAXIMUM_PIPELINE_DEALS_IN_FLIGHT", 18),
        required=False,
    )
    parser.add_argument(
        "--preferred-deal-size-bytes",
        help="The minimum piece size that you would prefer. Default 34359738368 bytes or 32GB",
        nargs="?",
        const=34359738368,
        type=int,
        default=os.environ.get("PREFERRED_DEAL_SIZE_BYTES", 34359738368),
        required=False,
    )
    parser.add_argument(
        "--complete-existing-deals-only",
        help="Setting this flag will prevent new deals from being requested but allow existing deals to complete. Useful for cleaning out the deals pipeline to debug, or otherwise. Default: False",
        nargs="?",
        const=False,
        type=bool,
        default=os.environ.get("COMPLETE_EXISTING_DEALS_ONLY", False),
        required=False,
    )
    parser.add_argument(
        "--verbose",
        help="If enabled, logging will be greatly increased. Default: False",
        nargs="?",
        const=False,
        type=bool,
        default=os.environ.get("VERBOSE", False),
        required=False,
    )
    parser.add_argument(
        "--debug",
        help="If enabled, logging will be thorough, enabling debugging of deep issues. Default: False",
        nargs="?",
        const=False,
        type=bool,
        default=os.environ.get("DEBUG", False),
        required=False,
    )
    return parser.parse_args()


def get_logger(name: str, *, options: dict) -> logging.Logger:
    logger = logging.getLogger(name)
    stdout_handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(f"%(asctime)s - %(levelname)s - {name}: %(message)s")
    stdout_handler.setFormatter(formatter)
    if options.debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    logger.addHandler(stdout_handler)
    return logger


options = parse_args()
json_rpc_id = 1
aria2 = None
log = get_logger("Ace", options=options)
log_retry = get_logger("RETRYING", options=options)
log_aria2 = get_logger("Aria2", options=options)
log_boost = get_logger("Boost", options=options)
log_request = get_logger("Request", options=options)

ELIGIBLE_PIECES_ENDPOINT = "https://api.spade.storage/sp/eligible_pieces"
INVOKE_ENDPOINT = "https://api.spade.storage/sp/invoke"
PENDING_PROPOSALS_ENDPOINT = "https://api.spade.storage/sp/pending_proposals"
ACTIVE_STATES = [
        # Boost Active States
        "Awaiting Offline Data Import",
        "Ready to Publish",
        "Awaiting Publish Confirmation",
        "Adding to Sector",
        "Announcing", 
        "Verifying Commp",
        "Transfer Queued",
        "Transferring",
        "Transfer stalled",
        # Sealing Active States
        "Sealer: WaitDeals",
	"Sealer: AddPiece",
	"Sealer: Packing",
	"Sealer: GetTicket",
	"Sealer: PreCommit1",
	"Sealer: PreCommit2",
	"Sealer: PreCommitting",
	"Sealer: PreCommitWait",
	"Sealer: SubmitPreCommitBatch",
	"Sealer: PreCommitBatchWait",
	"Sealer: WaitSeed",
	"Sealer: Committing",
	"Sealer: CommitFinalize",
	"Sealer: CommitFinalizeFailed",
	"Sealer: SubmitCommit",
	"Sealer: CommitWait",
	"Sealer: SubmitCommitAggregate",
	"Sealer: CommitAggregateWait",
	"Sealer: FinalizeSector",
        "Sealer: Sealing",
        #"Seaker: Proving",
        # Snap-up Active States
	"Sealer: SnapDealsWaitDeals",
	"Sealer: SnapDealsAddPiece",
	"Sealer: SnapDealsPacking",
	"Sealer: UpdateReplica",
	"Sealer: ProveReplicaUpdate",
	"Sealer: SubmitReplicaUpdate",
	"Sealer: ReplicaUpdateWait",
	"Sealer: FinalizeReplicaUpdate",
        #"Sealer: UpdateActivating",
		#"Sealer: ReleaseSectorKey",
    ]


def get_spid_token(*, options: dict, opt_payload: str) -> str:
    log.debug("Building SPID authentication token from lotus")
    b64_optional_payload = ""
    if opt_payload and opt_payload.strip():
        b64_optional_payload = base64.b64encode(opt_payload.encode()).decode()

    fullnode_api_info = os.getenv("FULLNODE_API_INFO")
    if not fullnode_api_info:
        raise ValueError("FULLNODE_API_INFOenvironment variable not set!")
    
    api_token, api_maddr = fullnode_api_info.split(":")
    _, api_proto, api_host, api_tproto, api_port, *_ = api_maddr.split("/")
    
    if api_proto == "ip6":
        api_host = f"[{api_host}]"
    
    lotus_url = f"http://{api_host}:{api_port}/rpc/v0"
    lotus_token = api_token
    
    b64_spacepad="ICAg"
    fil_genesis_unix = 1598306400
    fil_current_epoch = (int(time.time()) - fil_genesis_unix) // 30
    fil_finalized_tipset = lotus_request(url=lotus_url, token=lotus_token, method="Filecoin.ChainGetTipSetByHeight", params=[fil_current_epoch - 900, None])["Cids"]
    fil_finalized_worker_id = lotus_request(url=lotus_url, token=lotus_token, method="Filecoin.StateMinerInfo", params=[options.miner_id, fil_finalized_tipset])["Worker"]
    fil_current_drand_b64 = lotus_request(url=lotus_url, token=lotus_token, method="Filecoin.BeaconGetEntry", params=[fil_current_epoch])["Data"]
    fil_authsig = lotus_request(url=lotus_url, token=lotus_token, method="Filecoin.WalletSign", params=[fil_finalized_worker_id, f"{b64_spacepad}{fil_current_drand_b64}{b64_optional_payload}"])["Data"]

    spid_token = f"FIL-SPID-V0 {fil_current_epoch};{options.miner_id};{fil_authsig}"
    if b64_optional_payload:
        spid_token += f";{b64_optional_payload}"

    log.debug(f"SPID created: {spid_token}")
    
    return spid_token


def lotus_request(*, url: str, token: str, method: str, params: dict) -> bool:
    try:
        return make_lotus_request(url=url, token=token, method=method, params=params)
    except tenacity.RetryError as e:
        log.error("Retries failed. Moving on.")
        return None


@tenacity.retry(
    wait=tenacity.wait_exponential(min=1, max=6, multiplier=2),
    stop=tenacity.stop_after_attempt(5),
    after=tenacity.after.after_log(log_retry, logging.INFO),
)
def make_lotus_request(*, url: str, token: str, method: str, params: dict) -> Any:
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params
    }
    headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    log.debug(f"Making request to: {url} method: {method} params: {payload}")
    response = requests.post(
        url,
        headers=headers,
        json=payload
    )
    data = response.json()
    if 'error' in data:
        raise ValueError(f"Error executing '{payload}' against API {url}\n{data['error']}")
    return data["result"]


def eligible_pieces(*, options: dict) -> dict:
    log.debug("Querying for eligible pieces")
    headers = {"Authorization": get_spid_token(options=options, opt_payload="")}
    response = request_handler(
        url=ELIGIBLE_PIECES_ENDPOINT,
        method="get",
        parameters={"timeout": 30, "headers": headers, "allow_redirects": True},
    )
    if response == None:
        return None
    return response


def invoke_deal(*, piece_cid: str, tenant_policy_cid: str, options: dict) -> dict:
    log.debug("Invoking a new deal")
    payload = f"call=reserve_piece&piece_cid={piece_cid}&tenant_policy={tenant_policy_cid}"
    headers = {"Authorization": get_spid_token(options=options, opt_payload=payload)}
    response = request_handler(
        url=INVOKE_ENDPOINT, 
        method="post", 
        parameters={"timeout": 30, "headers": headers, "allow_redirects": True}
    )

    if response == None:
        return None
    if response["response_code"] == 403:
        return {
            "invocation_failure_slug": "ErrTooManyReplicas",
            "invocation_failure_message": "".join(response["error_lines"]),
        }

    response["piece_cid"] = re.findall(r"baga[a-zA-Z0-9]+", response["info_lines"][0])[0]
    log.debug(f"New deal requested: {response}")
    return response


def pending_proposals(*, options: dict) -> list:
    headers = {"Authorization": get_spid_token(options=options, opt_payload="")}
    response = request_handler(
        url=PENDING_PROPOSALS_ENDPOINT,
        method="get",
        parameters={"timeout": 30, "headers": headers, "allow_redirects": True},
    )
    if response == None:
        return None
    return response["response"]


def boost_import(*, options: dict, deal_uuid: str, file_path: str) -> bool:
    global json_rpc_id

    log.info(f"Importing deal to boost: UUID: {deal_uuid}, Path: {file_path}")
    boost_bearer_token = options.boost_api_info.split(":")[0]
    boost_url = options.boost_api_info.split(":")[1].split("/")[2]
    boost_port = options.boost_api_info.split(":")[1].split("/")[4]
    headers = {"Authorization": f"Bearer {boost_bearer_token}", "content-type": "application/json"}
    payload = {
        "method": "Filecoin.BoostOfflineDealWithData",
        "params": [
            deal_uuid,
            file_path,
            options.boost_delete_after_import,
        ],
        "jsonrpc": "2.0",
        "id": json_rpc_id,
    }

    response = request_handler(
        url=f"http://{boost_url}:{boost_port}/rpc/v0",
        method="post",
        parameters={"timeout": 30, "data": json.dumps(payload), "headers": headers},
    )
    json_rpc_id += 1
    if response == None:
        return None
    elif "error" in response:
        log.warning(f"Import to boost failed with error: {response['error']}")
        return None
    elif "result" in response and response["result"]["Accepted"]:
        log.info(f"Deal imported to boost: {deal_uuid}")
        return True
    else:
        log.error(f"Deal failed to be imported to boost: {response}")
        return None


def get_boost_deals(*, options: dict) -> Any:
    # ToDo: Filter out deals not managed by Spade by comparing against list of pending_proposals or filtering by the client address. Currently all deals are expected to be Spade deals.
    log.debug("Querying deals from boost")
    boost_url = options.boost_api_info.split(":")[1].split("/")[2]
    payload = {
        "query": "query { deals (limit: 100, filter: { IsOffline: true, Checkpoint: Accepted } ) { deals { ID CreatedAt Checkpoint IsOffline Err PieceCid Message } totalCount } }"
    }

    response = request_handler(
        url=f"http://{boost_url}:{options.boost_graphql_port}/graphql/query",
        method="post",
        parameters={"timeout": 30, "data": json.dumps(payload)},
    )
    if response == None:
        return None
    else:
        deals = response["data"]["deals"]["deals"]
        return [d for d in deals if d["Message"] == "Awaiting Offline Data Import"]


def get_in_process_deals(*, options: dict) -> Any:
    log.debug("Querying in process deals from boost")
    boost_url = options.boost_api_info.split(":")[1].split("/")[2]
    payload = {
        "query": "query { deals (limit: 100 ) { deals { ID PieceCid CreatedAt Message PieceSize  } totalCount } }"
    }
    response = request_handler(
        url=f"http://{boost_url}:{options.boost_graphql_port}/graphql/query",
        method="post",
        parameters={"timeout": 30, "data": json.dumps(payload)},
    )
    if response == None:
        return None
    else:
        deals = response["data"]["deals"]["deals"]
        return [d for d in deals if any(state.lower() in d["Message"].lower() for state in ACTIVE_STATES)]


def request_handler(*, url: str, method: str, parameters: dict) -> bool:
    try:
        return make_request(url=url, method=method, parameters=parameters)
    except tenacity.RetryError as e:
        log.error("Retries failed. Moving on.")
        return None


@tenacity.retry(
    wait=tenacity.wait_exponential(min=1, max=6, multiplier=2),
    stop=tenacity.stop_after_attempt(5),
    after=tenacity.after.after_log(log_retry, logging.INFO),
)
def make_request(*, url: str, method: str, parameters: dict) -> Any:
    log.debug(f"Making request to: {url} method: {method} params: {parameters}")
    try:
        if method == "post":
            response = requests.post(url, **parameters)
        if method == "get":
            response = requests.get(url, **parameters)

        res = response.json()
        # Disable logging of noisy responses
        if url != ELIGIBLE_PIECES_ENDPOINT:
            log_request.debug(f"Response: {res}")
    except requests.exceptions.HTTPError as e:
        log_request.error(f"HTTPError: {e}")
        raise Exception(f"HTTPError: {e}")
    except requests.exceptions.ConnectionError as e:
        log_request.error(f"ConnectionError: {e}")
        raise Exception(f"ConnectionError: {e}")
    except (TimeoutError, urllib3.exceptions.ReadTimeoutError, requests.exceptions.ReadTimeout) as e:
        log_request.error(f"Timeout: {e}")
        raise Exception(f"Timeout: {e}")
    except:
        log_request.error(f"Timeout: {e}")
        raise Exception(f"Timeout: {e}")

    if response.status_code == 401:
        if "error_lines" in res and "in the future" in "".join(res["error_lines"]):
            log_request.info(
                f'Known issue in Spade: the auth token generated by fil-spid.bash is "in the future" according to Spade. Retrying.'
            )
            raise Exception("Auth token is in the future.")
        else:
            log_request.error(f"401 Unauthorized: {res}")
            raise Exception(f"401 Unauthorized: {res}")
    if response.status_code == 403:
        if "error_slug" in res and res["error_slug"] == "ErrTooManyReplicas":
            return response
        else:
            log_request.error(f"403 Forbidden: {res}")
            raise Exception(f"403 Forbidden: {res}")
        

    return res


def download(*, options: dict, source: str) -> bool:
    try:
        return download_files(options=options, source=source)
    except tenacity.RetryError as e:
        log.error("Retries failed. Moving on.")
        return None


@tenacity.retry(
    wait=tenacity.wait_exponential(min=1, max=6, multiplier=2),
    stop=tenacity.stop_after_attempt(5),
    after=tenacity.after.after_log(log_retry, logging.WARN),
)
def download_files(*, options: dict, source: str) -> bool:
    if not source.startswith("http"):
        log_aria2.error(f"Error. Unknown file source: {source}")
        # ToDo handle this
    try:
        down = aria2.add_uris(
            [source],
            options={
                "max_connection_per_server": options.aria2c_connections_per_server,
                "auto_file_renaming": False,
                "dir": options.aria2c_download_path,
            },
        )
    except:
        log_aria2.error("Aria2c failed to start download of file.")
        raise Exception("Aria2c failed to start download of file.")

    # This is a hack. For some reason 'max_connection_per_server' is ignored by aria2.add_uris().
    # Since this is incredibly important for this use case, this workaround is required.
    down.options.max_connection_per_server = options.aria2c_connections_per_server
    log_aria2.info(f"Downloading file: {source}")
    return True


def get_downloads() -> dict:
    try:
        return get_aria_downloads()
    except tenacity.RetryError as e:
        log.error("Retries failed. Moving on.")
        return None


@tenacity.retry(
    wait=tenacity.wait_exponential(min=1, max=6, multiplier=2),
    stop=tenacity.stop_after_attempt(5),
    after=tenacity.after.after_log(log_retry, logging.WARN),
)
def get_aria_downloads() -> dict:
    return aria2.get_downloads()


def download_error(e, i):
    try:
        downloads = e.get_downloads()
        for down in downloads:
            if down.gid == i:
                log.info(f"Retrying download due to error: {down.error_message}")
                response = e.retry_downloads([down], False)
                log.debug(f"Retry status: {response}")
    except:
        log.error(f"Failed to retry download of {i}")


def shell(*, command: list, stdin: Optional[str] = None) -> str:
    # This is gross and unfortunately necessary. fil-spid.bash is too complex to reimplement in this script.
    try:
        process = subprocess.run(
            command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True, universal_newlines=True, input=stdin
        )
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            f"Bash command failed with exit code {e.returncode}",
            f"Error message: {e.stderr}",
        ) from e
    return process.stdout.rstrip()


def is_download_in_progress(*, url: str, downloads: list) -> bool:
    for d in downloads:
        for f in d.files:
            for u in f.uris:
                if u["uri"] == url:
                    return True
    return False


def request_deal(*, options: dict) -> Any:
    log.info("Requesting a new deal from Spade")
    # Select a deal
    pieces = eligible_pieces(options=options)
    if pieces == None:
        return None
    if len(pieces["response"]) < 1:
        log.error("Error. No deal pieces returned.")

    deal = {"padded_piece_size":0}
    # Randomly select a deal from those returned
    while deal["padded_piece_size"] < options.preferred_deal_size_bytes:
        deal = get_random_deal(pieces=pieces)
    log.debug(f"Deal selected: {deal}")

    # Create a reservation
    return invoke_deal(piece_cid=deal["piece_cid"], tenant_policy_cid=deal["tenant_policy_cid"], options=options)


def get_random_deal(pieces: Any) -> Any:
    deal_number = random.randint(0, len(pieces["response"]) - 1)
    return pieces["response"][deal_number]


def humanbytes(B):
    """Return the given bytes as a human friendly KB, MB, GB, or TB string."""
    B = float(B)
    KB = float(1024)
    MB = float(KB ** 2) # 1,048,576
    GB = float(KB ** 3) # 1,073,741,824
    TB = float(KB ** 4) # 1,099,511,627,776

    if B < KB:
        return '{0} {1}'.format(B,'Bytes' if 0 == B > 1 else 'Byte')
    elif KB <= B < MB:
        return '{0:.2f} KB'.format(B / KB)
    elif MB <= B < GB:
        return '{0:.2f} MB'.format(B / MB)
    elif GB <= B < TB:
        return '{0:.2f} GB'.format(B / GB)
    elif TB <= B:
        return '{0:.2f} TB'.format(B / TB)


def setup_aria2p(*, options: dict) -> Any:
    global aria2

    # Start an aria2c running process
    pid = os.getpid()
    aria2c_daemon = "aria2c --daemon --enable-rpc=true --rpc-listen-port=6800"
    aria2c_cmd = aria2c_daemon + " --stop-with-process=" + str(pid)
    try:
        output = subprocess.check_output(aria2c_cmd, shell=True, stderr=subprocess.STDOUT)
        print(output.decode())
    except subprocess.CalledProcessError as e:
        print(f"ERROR: failed to start aira2c daemon {e.returncode}:")
        print(e.output.decode())  # Print the output from the exception object

    time.sleep(5)

    try:
        aria2 = aria2p.API(
            aria2p.Client(
                host=":".join(options.aria2c_url.split(":")[:-1]), port=options.aria2c_url.split(":")[-1], secret=""
            )
        )
        aria2.get_global_options().max_concurrent_downloads = options.aria2c_max_concurrent_downloads
    except:
        log_aria2.error(f"Could not connect to an aria2 daemon running at '{options.aria2c_url}'")
        raise Exception(f"Could not connect to an aria2 daemon running at '{options.aria2c_url}'")

    try:
        aria2.listen_to_notifications(
            threaded=True,
            on_download_start=None,
            on_download_pause=None,
            on_download_stop=None,
            on_download_complete=None,
            on_download_error=download_error,
            on_bt_download_complete=None,
            timeout=5,
            handle_signals=True,
        )
    except:
        log_aria2.error(f"Could not start listening to notifications from aria2c API.")
        raise Exception(f"Could not start listening to notifications from aria2c API.")


def populate_startup_state(*, options: dict) -> dict:
    state = {}
    # Read in deals from Boost. This is the only source necessary on startup as
    # the main control loop will detect and update deal status dynamically.
    deals = get_boost_deals(options=options)
    for d in deals:
        state[d["PieceCid"]] = {
            "deal_uuid": d["ID"],
            "files": {},
            "timestamp_in_boost": time.time(),
            "status": "available_in_boost",
        }
    log.info(f"Found {len(deals)} deals in Boost")
    return state


def populate_pipeline_state(*, options: dict) -> Any:
    state = {
        "deals_in_pipeline": 0,
        "deals_in_pipeline_max": options.maximum_pipeline_deals_in_flight,
        "deals_pipeline_percent_full": 0
    }
    deals = get_in_process_deals(options=options)
    state["deals_in_pipeline"] = len(deals)
    state["deals_pipeline_percent_full"] =  f"{round((len(deals) / options.maximum_pipeline_deals_in_flight) * 100, 2)}%"  
    return state


def startup_checks(*, options: dict) -> None:
    # Ensure the download directory exists
    if not os.path.exists(options.aria2c_download_path):
        log.error(f"Aria2c download directory does not exist: {options.aria2c_download_path}")
        os._exit(1)

    if which("aria2c") is None:
        log.error(f"Error: Utility aria2c does not exist")
        os._exit(1)


def main() -> None:
    global options
    startup_checks(options=options)
    log.info("Connecting to Aria2c...")
    setup_aria2p(options=options)
    log.info(f"Starting Ace of Spades with options: {options}")

    deals_in_error_state = []
    state = populate_startup_state(options=options)
    pipeline_state = populate_pipeline_state(options=options)
    # Example: state = {
    #     '<piece_cid>': {
    #         'deal_uuid': '<deal_uuid>',
    #         'files': {
    #             'http://foo': 'incomplete',  # can be one of ['incomplete', '<file_path>']
    #             'http://foo2': 'incomplete',  # can be one of ['incomplete', '<file_path>']
    #         },
    #         'timestamp_in_boost': '1234567890',  # The timestamp at which Ace detected the deal in Boost. Used for weeding out an edge case of stale deals.
    #         'status': 'invoked'  #can be one of ['invoked','available_in_boost','downloading','downloaded']
    #     }
    # }

    # Control loop to take actions, verify outcomes, and otherwise manage Spade deals
    while True:
        pipeline_state = populate_pipeline_state(options=options)
        log.info("----------------------- RUNTIME LOG -----------------------")
        log.info(f"Pipeline: [Deals in Pipe: {pipeline_state['deals_in_pipeline']}] [Max in Pipe: {pipeline_state['deals_in_pipeline_max']}] [Pipe percent full: {pipeline_state['deals_pipeline_percent_full']}]")
        for cid in state.keys():
            deal = state[cid]
            log.info(f"Processing deal: [Piece Cid: {cid}] [Deal UUID: [{deal['deal_uuid']}] [Status: {deal['status']}]")
            index = 1
            for source in deal["files"].keys():
                log.info(f"File {index} of {len(deal['files'])}:")
                log.info(f"     [Source: {source}]")
                log.info(f"     {deal['files'][source]}")
                index += 1


        # Request deals from Spade
        if not options.complete_existing_deals_only:
            if len(state) < options.maximum_boost_deals_in_flight:
                deals_in_pipe = pipeline_state["deals_in_pipeline"] + options.maximum_boost_deals_in_flight
                if deals_in_pipe <= options.maximum_pipeline_deals_in_flight:
                    for i in range(len(state), options.maximum_boost_deals_in_flight):
                        new_deal = request_deal(options=options)
                        if new_deal != None:
                            if "invocation_failure_slug" in new_deal:
                                log.warning(
                                    f'Invoke failed due to Spade error. Slug: {new_deal["invocation_failure_slug"]}. Error: {new_deal["invocation_failure_message"]}'
                                )
                            else:
                                if new_deal["piece_cid"] not in deals_in_error_state:
                                    log.debug(f'adding {new_deal["piece_cid"]} to state')
                                    state[new_deal["piece_cid"]] = {
                                        "deal_uuid": "unknown",
                                        "files": {},
                                        "timestamp_in_boost": time.time(),
                                        "status": "invoked",
                                    }
                                    log.info(f'New deal found in Boost: {new_deal["piece_cid"]}')
                else:
                    log.info(f"Pipeline full waiting: [Needed Spots: {deals_in_pipe - options.maximum_pipeline_deals_in_flight}]")

        log.debug(f"request state: {json.dumps(state, indent=4)}")

        # Identify when deals are submitted to Boost
        deals = get_boost_deals(options=options)
        if deals != None:
            log.debug(f"Deals: {deals}")
            for d in deals:
                if d["PieceCid"] not in deals_in_error_state:
                    if d["PieceCid"] not in state:
                        # Fallback necessary during certain Ace restart scenarios
                        state[d["PieceCid"]] = {
                            "deal_uuid": d["ID"],
                            "files": {},
                            "timestamp_in_boost": time.time(),
                            "status": "available_in_boost",
                        }
                    if state[d["PieceCid"]]["status"] == "invoked":
                        state[d["PieceCid"]]["status"] = "available_in_boost"
                        state[d["PieceCid"]]["timestamp_in_boost"] = time.time()

        log.debug(f"identify state: {json.dumps(state, indent=4)}")

        # Edge case. If a deal shows up in Boost, but does not appear in Spade for more than 1 hour (3600 seconds),
        # consider the deal to be failed and remove it from consideration. Without this check, this failure scenario can
        # prevent Ace from maintaining its maximum_boost_deals_in_flight.
        for s in list(state):
            if state[s]["status"] == "available_in_boost":
                if state[s]["timestamp_in_boost"] < (time.time() - options.spade_deal_timeout):
                    log.warning(
                        f"Deal can be seen in Boost, but has not appeared in Spade for more than {options.spade_deal_timeout} seconds. Considering the deal to be either failed or not a Spade deal: {s}"
                    )
                    del state[s]
                    deals_in_error_state.append(s)

        proposals = pending_proposals(options=options)
        downloads = get_downloads()
        log.debug(f"Proposals: {proposals}")

        # Handle Spade errors in creating deals
        log.debug(f"deals_in_error_state: {deals_in_error_state}")
        if proposals != None:
            if "recent_failures" in proposals:
                for p in proposals["recent_failures"]:
                    if p["piece_cid"] in state:
                        log.warning(
                            f'Spade encountered an error with {p["piece_cid"]}: `{p["error"]}`. Ignoring deal and moving on.'
                        )
                        del state[p["piece_cid"]]
                        deals_in_error_state.append(p["piece_cid"])

        # Start downloading deal files
        if proposals != None and downloads != None:
            for p in proposals["pending_proposals"]:
                if p["piece_cid"] in state:
                    if state[p["piece_cid"]]["status"] == "available_in_boost":
                        state[p["piece_cid"]]["deal_uuid"] = p["deal_proposal_id"]

                        # Start download of all files in deal
                        running = 0
                        for source in p["data_sources"]:
                            # Notice and ingest preexisting downloads, whether by a previous Ace process or manual human intervention
                            if is_download_in_progress(url=source, downloads=downloads):
                                state[p["piece_cid"]]["files"][source] = "incomplete"
                                running += 1
                            else:
                                if download(source=source, options=options):
                                    running += 1
                                    state[p["piece_cid"]]["files"][source] = "incomplete"
                                else:
                                    log.error(f"Failed to start download of URL: {source}")
                        if running == len(p["data_sources"]):
                            state[p["piece_cid"]]["status"] = "downloading"

        log.debug(f"download state: {json.dumps(state, indent=4)}")

        # Check for completed downloads
        downloads = get_downloads()
        if downloads != None:
            for down in downloads:
                if down.is_complete == True:
                    for s in state.keys():
                        # Ensure files have been populated before proceeding
                        if bool(state[s]["files"]):
                            for source in state[s]["files"].keys():
                                # If the completed downloads's source matches this deal, change 'incomplete' to the file path
                                if source == down.files[0].uris[0]["uri"]:
                                    state[s]["files"][source] = str(down.files[0].path)
                                    break
                            #check state of all downloads
                            if all([options.aria2c_download_path in f for f in state[s]["files"].values()]):
                                state[s]["status"] = "downloaded"
                                log.info(f"Download complete: {s}")
                    # Cleanup download from Aria2c
                    down.purge()
                else:
                    for s in state.keys():
                        if bool(state[s]["files"]):
                            for source in state[s]["files"].keys():
                            # If the completed downloads's source matches this deal, change 'incomplete' to the current download status
                                if source == down.files[0].uris[0]["uri"]:
                                    state[s]["files"][source] = f"[Status: Incomplete] [Size: {humanbytes(down.total_length)}] [Downloaded: {humanbytes(down.completed_length)}] [Speed: {humanbytes(down.download_speed)}s] [Percent: {round(down.progress, 2)}%] [ETA: {down.eta}]"
                                    break

        log.debug(f"completed download state: {json.dumps(state, indent=4)}")

        # Import deals to Boost
        for s in list(state):
            outcome = []
            if state[s]["status"] == "downloaded":
                for f in state[s]["files"].keys():
                    out = boost_import(options=options, deal_uuid=state[s]["deal_uuid"], file_path=state[s]["files"][f])
                    if out != None:
                        outcome.append(out)

                # If all files for this deal have been imported, delete the deal from local state
                if all(outcome):
                    log.info(f"Deal complete: {s}")
                    del state[s]

        if options.complete_existing_deals_only:
            if len(state) == 0:
                log.info("No more deals in flight. Exiting due to --complete-existing-deals-only flag.")
                os._exit(0)

        log.info("-----------------------------------------------------------")
        time.sleep(60)

if __name__ == "__main__":
    main()
