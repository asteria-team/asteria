"""
Application entry point.
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import Tuple

import uvicorn
from fastapi import APIRouter, FastAPI

# TODO(Kyle): Remove for production
from resolver import mlops_root

sys.path.append(str(mlops_root().parent))

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
api_router = APIRouter(prefix="/api")

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
# Routes: Healthcheck
# -----------------------------------------------------------------------------


@api_router.get("/healthcheck")
async def get_healthcheck():
    return {"status": "healthy"}


# -----------------------------------------------------------------------------
# Routes: Read Results
# -----------------------------------------------------------------------------


# @g_app.get(
#     "/pdataset/{model_identifier}/{model_version}/{result_identifier}/{result_version}"
# )
# async def get_result_version(
#     model_identifier: str,
#     model_version: str,
#     result_identifier: str,
#     result_version: int,
# ):
#     """
#     Get an individual result version.
#     :param model_identifier: The identifier for the model of interest
#     :type model_identifier: str
#     :param model_version: The version string for the model of interest
#     :type model_version: str
#     :param result_identifier: The identifier for the result of interest
#     :type result_identifier: str
#     :param result_version: The version identifier for the result of interest
#     :type result_version: int
#     """
#     try:
#         # Read the result from the store
#         document = g_store.read_result(
#             model_identifier, model_version, result_identifier, result_version
#         )
#     except RuntimeError as e:
#         raise HTTPException(status_code=404, detail=f"{e}")
#     except Exception:
#         raise HTTPException(status_code=500, detail="Internal server error.")
#     return document

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

    g_app.include_router(api_router)
    uvicorn.run(g_app, host=host, port=port)

    return EXIT_SUCCESS


def main() -> int:
    datalake, host, port, verbose = parse_arguments()
    return run(datalake, host, port, verbose)


if __name__ == "__main__":
    sys.exit(main())
