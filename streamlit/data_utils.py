from pathlib import Path
import polars as pl
import duckdb


def combine_data_sources():
    lastfm_path = Path("data/lastfm")
    spotify_path = Path("data/spotify")

    # read in lastfm-data from parquet file
    lastfm_df = pl.read_parquet(
        lastfm_path / "lastfm-listening-2021-2026march.parquet"
    )
    lastfm_df = lastfm_df.unique(pl.col("date_played_unix"))

    # read in lastfm-data from recent data
    db = duckdb.read_json(lastfm_path/"listening/*.jsonl")
    lastfm_recent_df = db.pl()
    lastfm_recent_df = ( 
        lastfm_recent_df 
        .with_columns(
            pl.from_epoch(pl.col("date_played_unix"), time_unit="s").alias("track_played_utc")
        )
        .with_columns(
            artist_name = pl.col("artist_name").str.to_lowercase(),
            track_name = pl.col("track_name").str.to_lowercase(),
            album_name = pl.col("album_name").str.to_lowercase()
        )
    )

    # vstack parquet dataframe with recent dataframe 
    lastfm_df = lastfm_df.vstack(lastfm_recent_df)

    # enrich lastfm dataframe with columns in 'spotify_df'
    lastfm_df = lastfm_df.with_columns(spotify_track_uri=pl.lit(""))

    # setting up the spotify data
    spotify_df = pl.read_parquet(
        spotify_path / "2017-2021 spotify data.parquet"
    )

    # enrich the spotify dataframe with columns from 'lastfm_df'
    spotify_df = spotify_df.with_columns(
        artist_mbid=pl.lit(""), album_mbid=pl.lit(""), track_mbid=pl.lit("")
    )

    # rearrange the columns
    cols = [
        "track_played_utc",
        "date_played_unix",
        "track_name",
        "artist_name",
        "album_name",
        "track_mbid",
        "artist_mbid",
        "album_mbid",
        "spotify_track_uri",
    ]
    lastfm_df = lastfm_df.select(cols)
    spotify_df = spotify_df.select(cols)

    df = lastfm_df.vstack(spotify_df).sort(
        pl.col("date_played_unix"), descending=False
    )

    return df.filter(
        pl.col("artist_name").is_not_null()
    )


def main():
    df = combine_data_sources()
    print(df.shape)
    print(df.null_count())


if __name__ == "__main__":
    main()
