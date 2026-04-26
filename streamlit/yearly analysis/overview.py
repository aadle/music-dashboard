import streamlit as st
import polars as pl

st.set_page_config(layout="centered")
st.html("""
    <style>
        .stMainBlockContainer {
            max-width:50rem;
        }
    </style>
    """
)

df_full = st.session_state["scrobble_data"]

year_start = 2021
year_end = 2026
year_select = list(range(year_start, year_end+1))

with st.container():
    year = st.selectbox("year", year_select, index=len(year_select)-2)

df_current = (
    df_full
    .filter(pl.col("track_played_utc").dt.year() == year)
)
df_pre = df_full.filter(pl.col("track_played_utc").dt.year() < year)

st.title("årlig oversikt")

# key metrics
n_tracks_scrobbled = df_current.shape[0]

# discovery & variety
def unique_artists_tracks(df, year):
    return (
        df
        .filter(pl.col("track_played_utc").dt.year().is_in([year, year-1]))
        .group_by(pl.col("track_played_utc").dt.year())
        .agg(
            pl.col("artist_name").unique().len().alias("n_unique_artists"),
            pl.col("track_name").unique().len().alias("n_unique_tracks"),
            pl.len().alias("yearly_scrobbles")
        )
        .with_columns(adr=pl.col("n_unique_artists")/pl.col("yearly_scrobbles"))
        .sort("track_played_utc")
    )

unique_artists_tracks_diff = unique_artists_tracks(df_full, year)
current_year = unique_artists_tracks_diff.row(1, named=True)
prev_year = unique_artists_tracks_diff.row(0, named=True)

with st.container():
    t = st.columns(4)
    with t[0]:
        st.metric(
            "unike artister",
            current_year["n_unique_artists"],
            current_year["n_unique_artists"] - prev_year["n_unique_artists"],
        )
    with t[1]:
        st.metric(
            "unike låter",
            current_year["n_unique_tracks"],
            current_year["n_unique_tracks"] - prev_year["n_unique_tracks"],
        )
    with t[2]:
        st.metric(
            "antall låter spilt av",
            current_year["yearly_scrobbles"],
            current_year["yearly_scrobbles"] - prev_year["yearly_scrobbles"],
        )
    with t[3]:
        st.metric(
            "artist diversity ratio",
            f"{current_year["adr"]*100:.1f}%",
            f"{(current_year["adr"] - prev_year["adr"])*100:.1f}%",
            delta_color="inverse"

        )
st.caption(
    """
    'artist diversity ratio' regnes ut ved å ta antall unike artister man har
    spilt av over antall låter spilt av. Jo lavere scoren er, jo mer divers er
    lyttingen i sin helhet.
    """
)

# get listening data before the set year
def artist_newcomers(full_df, year):
    def unique_artist_scrobbles(df):
        return (
            df
            .group_by(pl.col("artist_name"))
            .agg(scrobbles=pl.len())
            .filter(pl.col("scrobbles") >= 5)
            .sort(pl.col("scrobbles"), descending=True)
        )

    df_curr = full_df.filter(pl.col("track_played_utc").dt.year() == year)
    df_prev = full_df.filter(pl.col("track_played_utc").dt.year() < year)

    unqiue_artists_curr = unique_artist_scrobbles(df_curr)
    unqiue_artists_prev = unique_artist_scrobbles(df_prev)

    newcomers = unqiue_artists_curr.join(
        unqiue_artists_prev, on="artist_name", how="anti"
    )

    return newcomers

df_artist_newcomers = artist_newcomers(df_full, year)
top_newcomer = df_artist_newcomers.row(0, named=True)

# topp artist
df_top_artists = (
    df_current
    .group_by(pl.col("artist_name"))
    .agg(scrobbles=pl.len())
    .sort("scrobbles", descending=True)
    .with_columns(pct_listening = pl.col("scrobbles")/pl.col("scrobbles").sum())
)
top_artist = df_top_artists.row(0, named=True)

st.subheader("artister")
artist_tables = st.toggle("vis tabeller", False)
artist_cols = st.columns(2)
# topp artist
with artist_cols[0]:
    st.metric("Mest avspilte artist", top_artist["artist_name"])
    if artist_tables:
        st.dataframe(df_top_artists)
# nykommer
with artist_cols[1]:
    st.metric("Årets nykommer", top_newcomer["artist_name"])
    if artist_tables:
        st.dataframe(df_artist_newcomers)
st.caption(
    """
    En nykommer i denne forstand er en artister som ikke har spilt i de 
    foregående årene.
    """
)

# topp låt
df_top_tracks = (
    df_current
    .group_by(pl.col("track_name"), pl.col("artist_name"))
    .agg(scrobbles=pl.len())
    .sort("scrobbles", descending=True)
)
top_track = df_top_tracks.row(0, named=True)
st.subheader("låter")
with st.container():
    st.metric("mest avspilte låt", 
              f"'{top_track["track_name"]}' av {top_track["artist_name"]}"
              )
    st.dataframe(df_top_tracks)
    
