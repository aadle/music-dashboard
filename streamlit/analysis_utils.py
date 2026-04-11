import polars as pl

def artist_ranking(df:pl.DataFrame, year:int=2025) -> pl.DataFrame:
    df = df.filter(
        pl.col("track_played_utc").dt.year() == year
    )

    # aggregate number of scrobbles per artist
    df = (
        df.group_by("artist_name")
        .agg(pl.len().alias(f"n_scrobbles_{year}"))
        .sort(f"n_scrobbles_{year}", descending=True)
        .with_row_index(f"rank_{year}", offset=1)
        .with_columns(pl.col(f"rank_{year}").cast(pl.Int64))
        .with_columns(pl.col(f"n_scrobbles_{year}").cast(pl.Int64))
    )

    return df


def listening_streak(df:pl.DataFrame, year:int=2025):
    df = df.filter(
        pl.col("track_played_utc").dt.year() == year
    )

    dates = df.select(
        pl.col("track_played_utc").dt.date().unique().alias("date")
    )

    listening_streak = dates.with_columns(
        is_break = pl.col("date").diff() != pl.duration(days=1) 
    ).with_columns( 
        streak_id = pl.col("is_break").fill_null(True).cum_sum()
    )

    streak_stats = (
        listening_streak
        .group_by(pl.col("streak_id"))
        .agg(
            start_date = pl.col("date").min(),
            end_date = pl.col("date").max(),
            streak_length = pl.len()
        )
    )
    streak_stats = streak_stats.sort(pl.col("streak_length"), descending=True)

    return streak_stats

def unique_artists(df, year:int=2025):
    if year < 2017:
        raise ValueError("No data before 2017 is available.")

    artists_year = (
        df
        .filter(pl.col("track_played_utc").dt.year() == year)
        .group_by(pl.col("artist_name"))
        .agg(
            scrobbles = pl.len()
        )
    )

    artists_else = (
        df
        .filter(pl.col("track_played_utc").dt.year() < year)
        .select(pl.col("artist_name").unique())
    )

    unique_artists_year = artists_year.join(artists_else, "artist_name", how="anti")
    return unique_artists_year.sort(pl.col("scrobbles"), descending=True)
