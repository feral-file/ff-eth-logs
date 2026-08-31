#!/usr/bin/env python3
"""Build manifest.json for a warehouse export (docs/operations.md §1.2).

The manifest is generated from the export's SOURCE (BigQuery + the GCS object
listing), never from downloaded files, so a partial or corrupted copy can never
match it. `backfill finish` refuses to publish coverage without it.

Usage:
    make_manifest.py <prefix> <export_end> <extract_date>

    prefix       export root, e.g. gs://eth-logs/eth-log-warehouse/v2
    export_end   the block captured at the top of warehouse_extract.sql
                 (the manifest's blocks.last; NEVER derived from the blocks
                 export itself — the live table advances while the script runs)
    extract_date YYYY-MM-DD of the extract run

Inputs, produced in the current directory before running (aliases may be
`rows` or `n_rows` — `rows` is reserved in some BigQuery contexts):

    bq query --use_legacy_sql=false --format=json --max_rows=100 \
      'SELECT DIV(block_number, 1000000) AS part, COUNT(*) AS n_rows
       FROM `indexer-eth-logs.eth_warehouse.logs` GROUP BY part ORDER BY part' \
      > parts.json

    bq query --use_legacy_sql=false --format=json \
      'SELECT COUNT(*) AS n_rows FROM `indexer-eth-logs.eth_warehouse.logs`' \
      > total.json

    bq query --use_legacy_sql=false --format=json \
      'SELECT MIN(number) AS first, MAX(number) AS last, COUNT(*) AS n_rows
       FROM `bigquery-public-data.crypto_ethereum.blocks`
       WHERE number <= <export_end>' \
      > blocks.json

    gcloud storage ls --json -r '<prefix>/**' > ls.json
    # (--json, not the removed --format=json flag)

Writes manifest.json to the current directory; upload it to the export root:
    gcloud storage cp manifest.json <prefix>/manifest.json
"""
import json
import sys


def rows(record):
    """Row count under either alias (`rows` is reserved in some BQ contexts)."""
    return int(record.get("n_rows", record.get("rows")))


def main():
    prefix, export_end, extract_date = (
        sys.argv[1].rstrip("/"),
        int(sys.argv[2]),
        sys.argv[3],
    )
    export_name = prefix.split("//", 1)[1].split("/", 1)[1]  # bucket stripped

    parts_rows = json.load(open("parts.json"))
    total_rows = json.load(open("total.json"))
    blocks_rows = json.load(open("blocks.json"))
    objects = json.load(open("ls.json"))

    parts = {f"{int(r['part']):03d}": rows(r) for r in parts_rows}
    # Every partition up to export_end's must have an entry, zero when empty:
    # the loader requires a logs/part=NNN directory for each one.
    for p in range(export_end // 1_000_000 + 1):
        parts.setdefault(f"{p:03d}", 0)
    parts = dict(sorted(parts.items()))

    blocks = blocks_rows[0]
    assert int(blocks["last"]) == export_end, (blocks, export_end)

    files = {}
    for o in objects:
        # `url` carries a #<generation> suffix in current gcloud output.
        url = (o.get("url") or o.get("storage_url") or "").split("#", 1)[0]
        if not url.endswith(".parquet"):
            continue
        assert url.startswith(prefix + "/"), url
        meta = o.get("metadata", o)
        md5 = meta.get("md5Hash") or meta.get("md5_hash")
        assert md5, o
        files[url[len(prefix) + 1 :]] = {"size": int(meta["size"]), "md5": md5}

    manifest = {
        "export": export_name,
        "source": f"bigquery-public-data.crypto_ethereum, extract of {extract_date}",
        "blocks": {
            "first": int(blocks["first"]),
            "last": int(blocks["last"]),
            "rows": rows(blocks),
        },
        "logs": {"rows": rows(total_rows[0]), "parts": parts},
        "files": files,
    }
    json.dump(manifest, open("manifest.json", "w"), indent=1)
    print(
        f"manifest.json: {len(files)} files, {manifest['logs']['rows']} logs, "
        f"blocks {blocks['first']}-{blocks['last']}, {len(parts)} partitions"
    )


if __name__ == "__main__":
    main()
