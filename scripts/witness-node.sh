#!/usr/bin/env bash
# A second, independent Bee node to use as the *witness* in
#   tests/test_localsync.py::test_live_witnessed_confirmation
#
# The witness only ever does one thing: GET /bytes/<ref> for a blob this
# machine just pushed, so the syncer can prove the network — not merely the
# uploading node's own disk — serves it. It needs no postage batch, no
# funding and no trust (the caller hashes every fetched byte against its
# reference), which is why it can run in Bee's download-only "ultra-light"
# mode: swap disabled, no chequebook, no xDAI, no xBZZ.
#
# A reverse proxy in front of your existing node is NOT a substitute: the
# fetch would be served from the uploader's own store and the test would
# pass while proving nothing.
#
# Usage:
#   scripts/witness-node.sh [data-dir]          # default: ~/.bee-witness
#   BEE_BIN=/path/to/bee scripts/witness-node.sh
#
# First start syncs the postage-contract state from the batch snapshot to
# the chain tip; /health answers "ok" long before that finishes, while
# /topology still returns 503 "Node is syncing". Measured on a fresh
# data-dir (Bee 2.8.2, public Gnosis RPC, 2026-09-22): ready ~12 minutes
# after start, then the witnessed-confirmation test passed in 23 s with a
# single connected peer. That warm-up is a one-time cost per data-dir —
# keep the directory and a later start is ready in ~15 s.
#
# The password below is bound to the key in $DATA_DIR/keys, so this script
# never rewrites an existing config: starting with a different password
# fails with "configure signer: swarm key: invalid password".
set -euo pipefail

DATA_DIR="${1:-$HOME/.bee-witness}"
API_PORT="${WITNESS_API_PORT:-1733}"
P2P_PORT="${WITNESS_P2P_PORT:-1734}"
RPC="${WITNESS_RPC:-https://rpc.gnosischain.com}"
PASSWORD="${WITNESS_PASSWORD:-swarmfs-witness}"

BEE="${BEE_BIN:-}"
if [ -z "$BEE" ]; then
  if command -v bee >/dev/null 2>&1; then
    BEE="$(command -v bee)"
  elif [ -x "$HOME/.local/share/Swarm Desktop/bee" ]; then
    BEE="$HOME/.local/share/Swarm Desktop/bee"   # Swarm Desktop ships one
  else
    echo "no bee binary found — install Bee or set BEE_BIN=/path/to/bee" >&2
    exit 1
  fi
fi

mkdir -p "$DATA_DIR"
CONFIG="$DATA_DIR/witness.yaml"
if [ -f "$CONFIG" ]; then
  # Never rewrite an existing config: the node's key in $DATA_DIR/keys is
  # encrypted with the password that created it, so a config with a
  # different one fails at startup with
  #   failed to build bee node ... configure signer: swarm key: invalid password
  # Delete the data-dir to start over, or edit the config by hand.
  echo "reusing the existing $CONFIG (delete the data-dir to start over)"
else
  cat > "$CONFIG" <<EOF
api-addr: 127.0.0.1:$API_PORT
p2p-addr: :$P2P_PORT
data-dir: $DATA_DIR
password: $PASSWORD
full-node: false
swap-enable: false
mainnet: true
storage-incentives-enable: false
blockchain-rpc-endpoint: $RPC
EOF
fi

cat <<EOF
Starting a download-only witness node
  bee:      $BEE
  data-dir: $DATA_DIR
  api:      http://127.0.0.1:$API_PORT

Wait until /topology answers 200 (not 503 "Node is syncing"):
  until curl -sf http://127.0.0.1:$API_PORT/topology >/dev/null; do sleep 10; done

Then, in another shell:
  SWARMFS_TEST_BEE=http://localhost:1633 \\
  SWARMFS_TEST_STAMP=<batch-id> \\
  SWARMFS_TEST_WITNESS=http://127.0.0.1:$API_PORT \\
  pytest tests/test_localsync.py::test_live_witnessed_confirmation

EOF

exec "$BEE" start --config="$CONFIG"
