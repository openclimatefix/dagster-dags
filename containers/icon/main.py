"""Entrypoint for calling either of Jacob's scripts.

They should be combined and streamlined but I'm loathe to touch them incase they
no longer work with the existing archive.
"""

import argparse
import os

from download_combine_upload_icon_eu import run as run_eu
from download_combine_upload_icon_global import run as run_global

parser = argparse.ArgumentParser()

parser.add_argument("area", choices=["eu", "global"])
parser.add_argument("--path", default="/tmp/nwp")

if __name__ == "__main__":
    print("Starting ICON download script")

    args = parser.parse_args()
    if args.area == "eu":
        run_eu(args.path)
    elif args.area == "global":
        run_global(args.path)
