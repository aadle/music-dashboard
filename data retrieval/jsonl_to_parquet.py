import duckdb
import polars as pl
from pathlib import Path


def main():
    data_dir = Path("../data/lastfm/listening")
    db = duckdb.read_json(data_dir / "*.jsonl")
    df = db.pl()
    df = df.with_columns(
        pl.from_epoch(pl.col("date_played_unix"), time_unit="s").alias(
            "track_played_utc"
        )
    )

    output_dir = Path("../data/lastfm")
    df.write_parquet(output_dir / "lastfm-listening-2021-2026.parquet")


if __name__ == "__main__":
    main()
