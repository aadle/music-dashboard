import streamlit as st
import plotly.graph_objects as go
import polars as pl

st.set_page_config(layout="centered")
st.html("""
    <style>
        .stMainBlockContainer {
            max-width:60rem;
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
    spilt av over antall låter spilt av. Jo lavere prosentandelen er, jo mer 
    mangfoldig er lyttingen i sin helhet.
    """
)


# -------------- Artister --------------
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
    .with_columns(pct_listening = pl.col("scrobbles")/pl.col("scrobbles").sum() * 100)
)
top_artist = df_top_artists.row(0, named=True)

with st.container():
    st.subheader("artister")
    artist_cols = st.columns(2)
    # topp artist
    with artist_cols[0]:
        st.metric("Mest avspilte artist", top_artist["artist_name"])
    # nykommer
    with artist_cols[1]:
        st.metric("Årets nykommer", top_newcomer["artist_name"])

with st.expander("vis tabeller"):
    artist_tables = st.columns(2)
    with st.container():
        with artist_tables[0]:
            st.caption("mest avspilte artister")
            st.dataframe(df_top_artists)
        with artist_tables[1]:
            st.caption("mest avspilte nykommere")
            st.dataframe(df_artist_newcomers)

st.caption(
    """
    En nykommer i denne forstand er artister som ikke har blitt spilt i de 
    foregående årene.
    """
)

# -------------- Låter --------------
# topp låt
df_top_tracks = (
    df_current
    .group_by(pl.col("track_name"), pl.col("artist_name"))
    .agg(scrobbles=pl.len())
    .sort([ "scrobbles", "artist_name" ], descending=[ True, False])

)
df_one_hit_pony = (
    df_current.group_by(pl.col("artist_name"))
    .agg(
        track_name=pl.col("track_name").unique().first(),
        scrobbles=pl.len(),
        unique_tracks=pl.col("track_name").unique().len(),
    )
    .filter(pl.col("unique_tracks") == 1)
    .filter(pl.col("scrobbles") > 2)
    .sort([ "scrobbles", "artist_name" ], descending=[ True, False])
    .drop("unique_tracks")
    .select("track_name", "artist_name", "scrobbles")
)

top_track = df_top_tracks.row(0, named=True)
one_hit_pony = df_one_hit_pony.row(0, named=True)
with st.container():
    st.subheader("låter")

    track_cols = st.columns(2)
    with track_cols[0]:
        st.metric("mest avspilte låt", 
                  f"'{top_track["track_name"]}'"
                  )
        st.caption(
            f"""
            '{top_track["track_name"]}' av {top_track["artist_name"]} har blitt
            avspilt {top_track["scrobbles"]} ganger.
            """
        )
    
    with track_cols[1]:
        st.metric(
            "årets one hit wonder",
            f"'{one_hit_pony['track_name']}'",
        )
        st.caption(
            f"""
            
            '{one_hit_pony["track_name"]}' av {one_hit_pony["artist_name"]} har blitt
            avspilt {one_hit_pony["scrobbles"]} ganger.
            """
        )

with st.expander("vis tabeller"):
    artist_tables = st.columns(2)
    with st.container():
        with artist_tables[0]:
            st.caption("mest avspilte låter")
            st.dataframe(df_top_tracks)
        with artist_tables[1]:
            st.caption("mest avspilte nykommere")
            st.dataframe(df_one_hit_pony)


st.caption(
    """
    En _one hit wonder_ i denne sammenheng er en artist som jeg kun har 
    spilt av én sang fra.
    """
)


# -------------- Plots --------------
st.header("plots")
df_monthly_scrobbles = (
    df_current
    .group_by(pl.col("track_played_utc").dt.month().alias("month"))
    .agg(scrobbles=pl.len())
    .sort("month", descending=False)
)

n_monthly = 10
df_top_n_monthly_tracks = (
    df_current
    .group_by(
        pl.col("track_played_utc").dt.month().alias("month"),
        "artist_name",
        "track_name",
    )
    .agg(scrobbles=pl.len())
    .sort(["month", "scrobbles"], descending=[False, True])
    .group_by("month")
    .head(n_monthly)
)

months_dict = {
    1: "januar",
    2: "februar",
    3: "mars",
    4: "april",
    5: "mai",
    6: "juni",
    7: "juli",
    8: "august",
    9: "september",
    10: "oktober",
    11: "november",
    12: "desember",
}

# Monthly bar chart with hover text
fig_monthly_histogram = go.Figure()
fig_monthly_histogram.update_layout(
    showlegend=False, 
    title=dict(text="månedlig fordeling")
)
months_list = df_monthly_scrobbles["month"].to_list()
hover_text = []

for month in months_list:
    entries = ( 
        df_top_n_monthly_tracks
        .filter(pl.col("month") == month)
        .rows(named=True) 
    )
    monthly_scrobbles = df_monthly_scrobbles.filter(pl.col("month") == month)["scrobbles"].item()
    x_month = months_dict.get(month)

    hover_text_ = f"""<b>antall låter spilt:</b> {monthly_scrobbles}<br><b>Top {n_monthly} tracks:</b><br>"""
    for idx, entry in enumerate(entries, 1):
        track_name = entry.get("track_name")
        artist_name = entry.get("artist_name")
        scrobbles = entry.get("scrobbles")
        
        hover_text_ += f"    <b>{idx}</b>. {track_name} ○ {artist_name} ({scrobbles} scrobbles)<br>"

    fig_monthly_histogram.add_trace(
        go.Bar(
            x=[x_month],
            y=[monthly_scrobbles],
            hovertext=hover_text_,
            hoverinfo="text",
            marker=dict(color="#29B09D")
        )
    )


# Weekly barplot
weekdays = {1: "mandag", 2: "tirsdag", 3: "onsdag", 4: "torsdag",
            5: "fredag", 6: "lørdag", 7: "søndag"}
df_weekly_scrobbles = ( 
    df_current
    .group_by(pl.col("track_played_utc").dt.weekday().alias("day"))
    .agg(scrobbles=pl.len())
    .sort("day", descending=False)
    .with_columns(
        day_name=pl.col("day").replace_strict(weekdays)
    )
)
fig_weekday = go.Figure(
    [
        go.Bar(
            y=df_weekly_scrobbles["scrobbles"],
            x=df_weekly_scrobbles["day_name"],
            marker=dict(color="#29B09D")
        )
    ]
)
fig_weekday.update_layout(title=dict(text="fordelt på ukedagene"))

# Hourly barplot
df_hourly_scrobbles = ( 
    df_current
    .group_by(pl.col("track_played_utc").dt.hour().alias("hour"))
    .agg(scrobbles=pl.len())
    .sort("hour", descending=False)
)
fig_hourly = go.Figure(
    [
        go.Bar(
            y=df_hourly_scrobbles["scrobbles"],
            x=df_hourly_scrobbles["hour"],
            marker=dict(color="#29B09D")
        )
    ]
)
fig_hourly.update_layout(title=dict(text="fordelt over døgnets timer"))

with st.container():
    st.plotly_chart(fig_monthly_histogram)
    weekday_hour_cols = st.columns(2)
    with weekday_hour_cols[0]:
        st.plotly_chart(fig_weekday)
    with weekday_hour_cols[1]:
        st.plotly_chart(fig_hourly)
