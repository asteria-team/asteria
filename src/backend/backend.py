"""
Application entry point.
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import Tuple

import uvicorn
from fastapi import APIRouter, FastAPI, HTTPException

# TODO(Kyle): Remove for production
from resolver import mlops_root

sys.path.append(str(mlops_root().parent))

from models import LogicalDatasetRequest, PhysicalDatasetRequest  # noqa

import mlops.datalake as dl  # noqa

# Application exit codes
EXIT_SUCCESS = 0
EXIT_FAILURE = 1

# The deafult host address to which the server binds
DEFAULT_HOST = "localhost"
# The default port on which the server listens
DEFAULT_PORT = 8080

# -----------------------------------------------------------------------------
# Global State
# -----------------------------------------------------------------------------

# The global FastAPI application
g_app = FastAPI()

# The router for all API routes
g_api_router = APIRouter(prefix="/api")

# -----------------------------------------------------------------------------
# Argument Parsing
# -----------------------------------------------------------------------------


def parse_arguments() -> Tuple[Path, str, int, bool]:
    """Parse commandline arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--datalake",
        type=str,
        required=True,
        help="The path to the data lake root.",
    )
    parser.add_argument(
        "--host",
        type=str,
        default=DEFAULT_HOST,
        help=f"The host address to which the server binds (default: {DEFAULT_HOST})",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=DEFAULT_PORT,
        help=f"The port on which the server listens (default: {DEFAULT_PORT})",
    )
    parser.add_argument(
        "--verbose", action="store_true", help="Enable verbose output."
    )
    args = parser.parse_args()
    return Path(args.datalake), args.host, args.port, args.verbose


# -----------------------------------------------------------------------------
# Route: Healthcheck
# -----------------------------------------------------------------------------


@g_api_router.get("/healthcheck")
async def get_healthcheck():
    return {"status": "healthy"}


# -----------------------------------------------------------------------------
# Route: Dataset Domain Metadata
# -----------------------------------------------------------------------------


@g_api_router.get("/datalake/metadata/dataset/domain")
async def get_dataset_domains():
    """
    Get the available dataset domain identifiers from the datalake.

    :return: {"domains": [{"identifier": <ID>}]}
    :rtype: Dict[str, Any]
    """
    try:
        domains = dl.dataset_domains().find({})
    except Exception:
        raise HTTPException(status_code=500, detail="Internal server error.")
    return {
        "domains": [{"identifier": domain.value.lower()} for domain in domains]
    }


# -----------------------------------------------------------------------------
# Route: Dataset Type Metadata
# -----------------------------------------------------------------------------


@g_api_router.get("/datalake/metadata/dataset/type")
async def get_dataset_types():
    """
    Get the available dataset type identifiers from the datalake.

    :return: {"types": [{"identifier": <ID>}]}
    :rtype: Dict[str, Any]
    """
    try:
        types = dl.dataset_types().find({})
    except Exception:
        raise HTTPException(status_code=500, detail="Internal server error.")
    return {"types": [{"identifier": t.value.lower()} for t in types]}


# -----------------------------------------------------------------------------
# Route: Physical Dataset Request
# -----------------------------------------------------------------------------


@g_api_router.post("/datalake/dataset/pdreq")
async def get_pdatasets(pdataset_request: PhysicalDatasetRequest):
    """
    Get all physical datasets matching provided query.

    :param pdataset_request: The DQL query document
    :type pdataset_request: PhysicalDatasetRequest

    :return: {"datasets": [{ ... metadata ... }]}
    :rtype: Dict[str, Any]
    """
    try:
        views = dl.pdatasets().find(pdataset_request.query)
    except dl.ParseError as e:
        raise HTTPException(status_code=404, detail=f"Invalid query: {e}")
    except dl.EvaluationError as e:
        raise HTTPException(status_code=404, detail=f"Invalid query: {e}")
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Internal server error: {e}"
        )

    return {"datasets": [view.metadata.to_json() for view in views]}


# -----------------------------------------------------------------------------
# Route: Logical Dataset Request
# -----------------------------------------------------------------------------


@g_api_router.post("/datalake/dataset/ldreq")
async def get_ldatasets(ldataset_request: LogicalDatasetRequest):
    """
    Get all logical datasets matching provided query.

    :param ldataset_request: The DQL query document
    :rtype: LogicalDatasetRequest

    :return: {"datasets": [{ ... metadata ... }]}
    :rtype: Dict[str, Any]
    """
    try:
        views = dl.ldatasets().find(ldataset_request.query)
    except dl.ParseError as e:
        raise HTTPException(status_code=404, detail=f"Invalid query: {e}")
    except dl.EvaluationError as e:
        raise HTTPException(status_code=404, detail=f"Invalid query: {e}")
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Internal server error: {e}"
        )
    return {"datasets": [view.metadata.to_json() for view in views]}


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------


def run(
    datalake: Path,
    host: str,
    port: int,
    verbose: bool,
) -> int:
    """
    Run the server.

    :param datalake: The path to the data lake root
    :type datalake: Path
    :param host: The host address to which the server binds
    :type host: str
    :param port: The port to which the server binds
    :type port: int
    :param verbose: Enable verbose logging
    :type verbose: bool

    :return: An error code
    :rtype: int
    """
    global g_app

    logging.basicConfig(level=logging.INFO if verbose else logging.ERROR)

    # Initialize the data lake
    if not datalake.is_dir():
        raise RuntimeError(f"{datalake} is not a suitable data lake location.")
    dl.set_path(datalake)

    g_app.include_router(g_api_router)
    uvicorn.run(g_app, host=host, port=port)

    return EXIT_SUCCESS


def main() -> int:
    datalake, host, port, verbose = parse_arguments()
    return run(datalake, host, port, verbose)


if __name__ == "__main__":
    sys.exit(main())
