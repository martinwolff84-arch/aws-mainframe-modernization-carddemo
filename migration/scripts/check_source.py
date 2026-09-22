"""Verify the exact vendored source snapshot; no network or target code needed."""
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
manifest = json.loads((ROOT / "upstream/manifest.json").read_text())
for name, expected in manifest["sha256"].items():
    actual = hashlib.sha256((ROOT / "upstream" / name).read_bytes()).hexdigest()
    if actual != expected:
        raise SystemExit(f"Upstream source mismatch: {name}")
print(f"Verified {len(manifest['sha256'])} upstream files at {manifest['commit']}")
acceptance = json.loads((ROOT / "evidence/acceptance-manifest.json").read_text())
for name, expected in acceptance["sha256"].items():
    actual = hashlib.sha256((ROOT / name).read_bytes()).hexdigest()
    if actual != expected:
        raise SystemExit(f"Frozen acceptance mismatch: {name}; review any apparatus change explicitly")
print(f"Verified {len(acceptance['sha256'])} frozen acceptance files and {len(acceptance['cases'])} case identities")
