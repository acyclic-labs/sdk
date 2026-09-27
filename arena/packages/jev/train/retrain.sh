#!/usr/bin/env bash
# Retrain the System One scorer WITHOUT the CC-BY-NC ticket family, so the
# adapter can be used commercially. Runs the model author's published training
# script (downloaded at run time; it is not vendored here) on a GPU box.
#
#   HF_TOKEN=... PUSH_REPO=you/system-one-commercial ./retrain.sh
#
# Needs: one A100/H100-class GPU (~3 h), python3 with torch, transformers==5.17.0,
# peft==0.21.0, accelerate, datasets. Env knobs: OUT_DIR, DATA_DIR, MAX_STEPS,
# BASE_MODEL, PUSH_REPO (unset = do not push), SIZES (JSON per family).
set -euo pipefail

# The training script is pinned to a commit and its SHA-256 is checked before it runs, so a change
# upstream (or a compromised account) cannot run arbitrary code on this host. To move the pin, set both.
SCRIPT_REV="${SCRIPT_REV:-e6464dce15f013c2ef641593a85cc6afcdaea928}"
SCRIPT_SHA256="${SCRIPT_SHA256:-cd9865bc82e1b49972955986e74856f10f0a66d4b9d057114958d1b4625906ff}"
SCRIPT_URL="https://huggingface.co/pngwn/system-one-qwen3.5-4b-scorer/raw/$SCRIPT_REV/system_one.py"
OUT_DIR="${OUT_DIR:-/tmp/system-one-commercial}"
DATA_DIR="${DATA_DIR:-$OUT_DIR/data}"
BASE_MODEL="${BASE_MODEL:-Qwen/Qwen3.5-4B-Base}"
MAX_STEPS="${MAX_STEPS:-2200}"
# Families and their licenses. tickets (Tobi-Bueck/customer-support-tickets, CC-BY-NC-4.0) is
# deliberately absent: that is the whole point of this retrain.
#   banking77   CC-BY-4.0      go_emotions  Apache-2.0   ag_news  custom/non-restrictive
#   mmlu        MIT            yelp_score   Yelp dataset terms (check before commercial use; set to 0 to drop)
SIZES="${SIZES:-{\"banking77\":[3000,300,300],\"go_emotions\":[3000,300,300],\"ag_news\":[2000,200,200],\"mmlu\":[2000,200,200],\"yelp_score\":[2000,200,200],\"tickets\":[0,0,0]}}"

mkdir -p "$OUT_DIR"
cd "$OUT_DIR"
curl -sSL --proto '=https' --tlsv1.2 "$SCRIPT_URL" -o system_one.py
echo "$SCRIPT_SHA256  system_one.py" | sha256sum --check --status || { echo "system_one.py does not match the pinned SHA-256; refusing to run it" >&2; exit 1; }
python3 -c "import torch, transformers, peft, datasets; print('torch', torch.__version__, 'cuda', torch.cuda.is_available())"

echo "== build (tickets excluded)"
python3 system_one.py build --sizes "$SIZES" --local-data-dir "$DATA_DIR"

echo "== train + calibrate"
PUSH=()
if [[ -n "${PUSH_REPO:-}" ]]; then PUSH=(--push --push-repo "$PUSH_REPO"); fi
python3 system_one.py train --base-model "$BASE_MODEL" --local-data-dir "$DATA_DIR" \
  --out-dir "$OUT_DIR/run" --max-len 384 --max-options 16 --batch-size 8 --lr 1e-4 \
  --max-steps "$MAX_STEPS" --grad-checkpointing --eval-test "${PUSH[@]}"

echo "== done: adapter in $OUT_DIR/run; temperature and metrics in its metrics.json"
echo "Use it with:  SYSTEM_ONE_ADAPTER=$OUT_DIR/run SYSTEM_ONE_TEMPERATURE=<fitted T> jev serve --backend local"
