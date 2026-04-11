from pathlib import Path
import polars as pl


def combine_data_sources():
    lastfm_path = Path("data/lastfm")
    spotify_path = Path("data/spotify")

    # read in lastfm-data
    lastfm_df = pl.read_parquet(
        lastfm_path / "lastfm-listening-2021-2026march.parquet"
    )
    lastfm_df = lastfm_df.unique(pl.col("date_played_unix"))

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
    )  # some entries have no data except track played. We do not take them into consideration


def main():
    df = combine_data_sources()
    print(df.shape)
    print(df.null_count())


if __name__ == "__main__":
    main()
