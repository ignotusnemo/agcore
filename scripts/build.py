#!/usr/bin/env python3
"""Build with explicit, authenticated Parano1d release material."""
import argparse
import json
import os
from pathlib import Path
import subprocess

KEYS = {
    "NOID_HISTORY_STEP_PACK_DIR", "NOID_HISTORY_STEP_RUNTIME_METADATA_RELEASE_DIGEST",
    "NOID_V2_PACK_DIR", "NOID_V2_RELEASE_BANK", "NOID_RETIREMENT_KEYS_DIR",
    "NOID_RETIREMENT_KEY_0_PIN", "NOID_RETIREMENT_KEY_1_PIN",
}

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--inputs", type=Path, required=True, help="public build-inputs JSON with profile and environment")
parser.add_argument("--retired-history", action="store_true")
parser.add_argument("--jobs", type=int, default=2)
args = parser.parse_args()
inputs = json.loads(args.inputs.read_text())
profile = inputs["profile"]
if profile not in ("mainnet", "isolated-v2-fork-testnet"):
    raise SystemExit("unsupported network profile")
if set(inputs["environment"]) != KEYS or args.jobs < 1:
    raise SystemExit("provide exactly the seven proof artifact inputs and a positive job count")
environment = os.environ.copy()
environment.update(inputs["environment"])
features = [] if profile == "mainnet" else [profile]
if args.retired_history:
    features.append("retired-history")
command = ["cargo", "build", "--release", "--locked", "-j", str(args.jobs)]
if features:
    command += ["--features", ",".join(features)]
subprocess.run(command, cwd=Path(__file__).resolve().parents[1], env=environment, check=True)
