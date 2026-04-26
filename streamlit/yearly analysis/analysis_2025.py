import streamlit as st
import polars as pl
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from scipy.signal import savgol_filter
from scipy.fft import dct, idct 
from analysis_utils import artist_ranking, listening_streak

st.set_page_config(layout="centered")
st.html("""
    <style>
        .stMainBlockContainer {
            max-width:50rem;
        }
    </style>
    """
)

table_height = 283

year = 2025
df_raw = st.session_state["scrobble_data"]
df = df_raw.filter(pl.col("track_played_utc").dt.year() == year)
# df = df_raw.with_columns(
#     pl.col("track_played_utc").dt.convert_time_zone(time_zone="Europe/Berlin")
# ).filter(pl.col("track_played_utc").dt.year() == year) # timezone aware

# ---
st.title("Lyden av 2025")
st.markdown(
    """
    _Dette er på ingen måte en blodseriøs analyse. Dette er kun for gøy og
    er av egen nysgjerrighet._
    """
)

# ---
st.header("2025 sammenlignet årene før")
# Change in listening
df_pct_change = ( 
  df_raw
  .group_by(pl.col("track_played_utc").dt.year().alias("year"))
  .agg(total_scrobbles = pl.len())
  .filter(pl.col("year").is_in([x for x in range(2021, 2025+1)]))
  .sort(pl.col("year"), descending=False)
  .with_columns(pct_change=pl.col("total_scrobbles").pct_change())
  .sort(pl.col("year"), descending=True)
)

df_2025_diff = df_pct_change.filter(pl.col("year") == 2025)
df_2024_diff = df_pct_change.filter(pl.col("year") == 2024)
scrobbles_cols = st.columns(3)

with scrobbles_cols[0]:
    st.metric(
        "Antall scrobbles i 2024",
        df_2024_diff["total_scrobbles"].item(),
    )
with scrobbles_cols[1]:
    st.metric(
        "Antall scrobbles i 2025",
        df_2025_diff["total_scrobbles"].item(),
        f"{df_2025_diff["pct_change"].item()*100:.1f}%",
    )
if st.toggle("_Vis tabell over endring i scrobbles_"):
    st.dataframe(df_pct_change, height=table_height)

st.markdown(
    """
    Ulikt Spotify teller Last.fm i "scrobbles" som tilsier at du har hørt på en låt, 
    og skiller seg derfor fra Spotify sine antall minutter som de viser i sin 
    "Wrapped".

    I 2025 har lyttingen en nedgang i antall scrobbles på 18% fra året før og er det 
    laveste tallet de siste 5 årene. Ved min egen oppfatning er ikke denne nedgangen 
    noe overraskende da jeg har forsøkt å kutte ut musikk som bakgrunnstøy når jeg 
    gjør noe f.eks. jobbe med noe studierelatert, diverse husarbeid, surfe på nettet 
    eller er ute og går.
    """
)

# ---
st.header("Lyttetrend utover 2025")
df_trend = (
    df
    .group_by(pl.col("track_played_utc").dt.date())
    .agg(n_scrobbles=pl.len())
    .sort(pl.col("track_played_utc"), descending=False)
    .with_columns(weekday = pl.col("track_played_utc").dt.strftime("%A"))
)

signal = df_trend.select("n_scrobbles").to_numpy().flatten()
filtered_signal = savgol_filter(signal, window_length=15, polyorder=3)
df_trend = df_trend.with_columns(svg_trend_scrobbles=filtered_signal)

# Alternatively, use DCT to smooth out the signal to retrieve the trend
fourier_signal = dct(df_trend["n_scrobbles"].to_numpy().flatten())
W = np.arange(0, fourier_signal.shape[0])
filtered_fourier_signal = fourier_signal.copy()
filtered_fourier_signal[(W>70)] = 0
cut_signal = idct(filtered_fourier_signal)
df_trend = df_trend.with_columns(dct_trend_scrobbles=cut_signal)
 
fig_trend = px.line(
    df_trend,
    x="track_played_utc",
    y=["n_scrobbles", "svg_trend_scrobbles", "dct_trend_scrobbles"],
)

st.plotly_chart(fig_trend)
st.markdown(
    """
    Ved å bruke Savitzky-Golay filter, eller bruke et lowpass filter ved bruk av 
    Direct Cosine Transform, kan man tydeliggjøre trenden over hvordan lyttemønsteret 
    mitt har utviklet seg utover året.

    Mesteparten av av lyttingen er blitt unnagjort i den første halvdelen av året. 
    Naturlig kommer dette av at jeg jobbet med masteroppgaven min i disse månedene,
    hvor det ser en stor nedgang i aktivitet utover junimåned med færre forpliktelser 
    (sett gjennom `dct_trend_scrobbles` eller `svg_trend_scrobbles`). 

    Som tidligere nevnt ovenfor kan nedgangen være forårsaket av fokuset på å ikke ha 
    noe i øret til en hver tid. Nedgangen kan også skyldes at tiden som ellers hadde
    blitt brukt på musikk har gått over til noe annet, som podcasts for eksempel 
    eller støy, som Last.fm ikke tar hensyn til.

    Det som er interessant å bemerke seg er det repeterende mønsteret i andre 
    halvdel av året hvor man har en topp etterfulgt av en mindre topp som så daler 
    ned i bunn fra august av.
    """
)

# ---
st.subheader("Lyttingen sett gjennom stolper")

# Monthly histogram
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

df_monthly_hist = (
    df.group_by(pl.col("track_played_utc").dt.month().alias("month_idx"))
    .agg(scrobbles=pl.len())
    .with_columns(
        month=pl.col("month_idx")
        .replace_strict(months_dict, default=None)
    )
    .select("month_idx", "month", "scrobbles")
    .sort("month_idx", descending=False)
)

fig_monthly_hist = px.histogram(
    df_monthly_hist, 
    x="month", 
    y="scrobbles", 
    nbins=7, 
    text_auto=".0f",
    category_orders={"month": list(months_dict.values())},
    title="Frekvens av scrobbles i løpet av året"
)
fig_monthly_hist.update_layout(
    bargap=0.2, xaxis=dict(title="måned"), yaxis=dict(title="antall scrobbles")
)
fig_monthly_hist.update_traces(
    textfont_size=10, textangle=0, textposition="outside", cliponaxis=False,
    marker_color="#35897D"
)
st.plotly_chart(fig_monthly_hist)
st.markdown(
    """
    Stolpediagrammet her reflekterer i stor grad trendkurven fra plottet over.
    Til tross for at den mest lyttede dagen befinner seg i juni hørte jeg på
    drastisk mindre musikk den måneden sammenlignet de fem foregående.
    Musikk-konsumet halverte seg i siste halvdel av året.
    """
)


# daily histogram
weekdays = {1: "mandag", 2: "tirsdag", 3: "onsdag", 4: "torsdag",
            5: "fredag", 6: "lørdag", 7: "søndag"}

df_daily_hist = (df
    .group_by(pl.col("track_played_utc").dt.weekday().alias("weekday_idx"))
    .agg(scrobbles_by_weekday = pl.len())
    .sort("scrobbles_by_weekday", descending=True)
    .with_columns(weekday = pl.col("weekday_idx").replace_strict(weekdays, default=None).alias("weekday"))
)

fig_daily_hist = px.histogram(
    df_daily_hist, 
    x="weekday", 
    y="scrobbles_by_weekday", 
    nbins=7, 
    text_auto=".0f",
    category_orders={"weekday": list(weekdays.values())},
    title="Frekvens av scrobbles i ukedagene"
)
fig_daily_hist.update_layout(
    bargap=0.2, xaxis=dict(title="ukedag"), yaxis=dict(title="antall scrobbles")
)
fig_daily_hist.update_traces(
    textfont_size=10, textangle=0, textposition="outside", cliponaxis=False,
    marker_color="#35897D"
)


# hourly histogram
df_peak_hours = (
    df
    .group_by(
        pl.col("track_played_utc")
        .dt.convert_time_zone(time_zone="Europe/Berlin")
        .dt.hour().alias("hour")
    )
    .agg(scrobbles_by_hour = pl.len()) 
    .sort("scrobbles_by_hour", descending=True)
)

fig_hour_hist = px.histogram(
    df_peak_hours,
    x="hour",
    y="scrobbles_by_hour",
    nbins=24,
    text_auto=".0f",
    title="Frekvens av scrobbles gjennom døgnets timer",
)
fig_hour_hist.update_layout(
    bargap=0.2, xaxis=dict(title="time"), yaxis=dict(title="")
)
fig_hour_hist.update_traces(
    textfont_size=10, textangle=0, textposition="outside", cliponaxis=False,
    marker_color="#35897D"
)

monthly_col, weekday_col = st.columns([.35, .65])

with monthly_col:
    st.plotly_chart(fig_daily_hist)
with weekday_col:
    st.plotly_chart(fig_hour_hist)

st.markdown(
    """
    Av hverdagene (mandag-fredag) er det tirsdag som ikke helt er på samme nivå som 
    resten. Lørdag og søndag derimot ser vi en sterk nedgang i musikklytting, men
    dette er ikke så rart med tanke på at jeg har brukt musikk i stor grad som
    bakgrunnsstøy.
    """
)

st.markdown(
    """
    Ut ifra den fordelingen av døgnets timer er det klart at mesteparten av 
    lyttingen foregår i nitiden fram firetiden, etterfulgt av en periode fra fire 
    til syv på ettermiddagen; vi har en bimodal fordeling hvor det er størst 
    aktivitet på formiddagen etterfulgt av en mindre aktiv periode på 
    ettermiddagstid.
    """
)


df_trend_stats = ( 
  df_trend.select(
    pl.col("n_scrobbles").mean().alias("mean_scrobbles"),
    pl.col("n_scrobbles").std().alias("std_scrobbles"),
    pl.col("n_scrobbles").median().alias("median_scrobbles").cast(int),
  )
)

mean_value = df_trend_stats["mean_scrobbles"][0]
median_value = df_trend_stats["median_scrobbles"][0]
fig_scrobble_freq = px.histogram(
    df_trend, 
    x="n_scrobbles", 
    nbins=20,
    title="Histogram over antall låter spilt på en dag"
    )
fig_scrobble_freq.update_traces(marker_color="#35897D")
fig_scrobble_freq.update_layout(
  bargap=0.2,
  xaxis=dict(title="antall scrobbles"),
  yaxis=dict(title="frekvens av scrobbles per dag")
  )
fig_scrobble_freq.add_vline(
    x=mean_value,
    line_dash="dash",
    line_color="red",
    annotation_text=f"Gjennomsnitt: {mean_value:.2f}",
    annotation_position="top right"
)
fig_scrobble_freq.add_vline(
    x=median_value,
    line_dash="dash",
    line_color="black",
    annotation_text=f"Median: {median_value:.2f}",
    annotation_position="top left"
)

st.plotly_chart(fig_scrobble_freq)
st.markdown(
    """
    Histogrammet viser at fordelingen er skjev med topp mot venstresiden og 
    en hale mot høyre. Vi ser at det er av det sjeldnere slag at jeg lytter 
    gjennom 70 låter på én dag.

    I gjennomsnitt har jeg lyttet på 55 låter i året, med en median på 44. Det
    hadde vært interessant å regne ut hvor lang en gjennomsnittlig låt er i mitt
    bibliotek/av det jeg har spilt av (_TODO?_). Hvis vi slenger ut et tall –
    la oss si 3 minutter – ligger vi på 165 minutter, eller 2 timer og 45
    minutter, i gjennomsnitt.
    """
)



st.header("Sammenhengende lytting")
st.markdown(
    """
    *_TODO_*: _regn ut hvor hvor mange "sessions jeg har i løpet av en dag. finn
    hvilken dag jeg har lengst session. Se i `listening sessions.ipynb`"_
    """
)

st.subheader("Uavbruttede rekker med musikklytting")
st.write(listening_streak(df, year))
st.markdown(
    """
    Dette er for meg overraskende. At jeg har i 212 sammenhengende dager åpnet 
    opp Spotify-appen og spilt en låt er både urovekkende og imponerende. 
    Men at det kun er totalt **2 dager av året** hvor jeg ikke har åpnet opp appen,
    enten på laptopen eller telefonen er enda mer urovekkende. 

    Man kan trygt på
    dette tidspunktet å fastslå at det musikken er en del av min hverdag dersom
    det skulle være noe tvil ut ifra det jeg allerede har lagt fram.

    For å sette rekkene i perspektiv kan vi se på heatmappet under:
    """
)

df_heatmap = (
    df
    .group_by(pl.col("track_played_utc").dt.date())
    .agg(
      tracks_scrobbled=pl.len()
    )
    .sort(pl.col("track_played_utc"), descending=False)
    .with_columns(
        week=pl.col("track_played_utc").dt.strftime("%W").cast(int),
        weekday=pl.col("track_played_utc").dt.weekday(),
        date_str=pl.col("track_played_utc").dt.strftime("%Y-%m-%d"),
    )
)

min_week = df_heatmap["week"].min()
max_week = df_heatmap["week"].max()

all_weeks = list(range(min_week, max_week + 1))

fig_heatmap = go.Figure(
    data=go.Heatmap(
        z=df_heatmap["tracks_scrobbled"],
        x=df_heatmap["week"],
        y=df_heatmap["weekday"],
        colorscale="Emrld",
        showscale=True,
        xgap=3,
        ygap=3,
        hovertext=df_heatmap["date_str"],
        hovertemplate="<b>Date:</b> %{hovertext}<br><b># Scrobbles:</b> %{z}<br><b>Uke:</b> %{x}<extra></extra>",
    )
)

fig_heatmap.update_layout(
    title=f"{year} lytteaktivitet",
    height=200,
    width=800,
    xaxis=dict(
        showgrid=False,
        zeroline=False,
        tickmode="array",
        tickvals=all_weeks[::3],
        ticktext=[f"{w+1}" for w in all_weeks[::3]],
        tickfont=dict(size=10),
    ),
    yaxis=dict(
        showgrid=False,
        zeroline=False,
        autorange="reversed",
        tickvals=[x for x in range(1, 8)],
        ticktext=["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"],
        tickmode="array",
        scaleanchor="x",
        side="left"
    ),
    plot_bgcolor="rgba(0,0,0,0)",
    margin=dict(t=40, b=40, l=40, r=40),
)
st.plotly_chart(fig_heatmap)
st.caption(
    """
    Dette ser mye bedre ut enn [min GitHub contribution graph](https://github.com/aadle).
    Finn de tomme rutene!
    """
)


st.subheader("Lyttesessions")

df_listening_deltas = (
    df.sort(pl.col("date_played_unix"), descending=False)
    .filter(pl.col("track_played_utc").dt.year() == year)
    .with_columns(
        played_delta_s = pl.col("date_played_unix").diff().fill_null(0)
    )
    .sort(pl.col("played_delta_s"), descending=True)   
)
session_threshold = 15*60 # 15 minutes

df_sessions = (
    df_listening_deltas.sort(pl.col("date_played_unix"), descending=False)
    .with_columns((pl.col("played_delta_s") > session_threshold).alias("exceeds"))
    .with_columns(pl.col("exceeds").cum_sum().alias("nth_session"))
    # best way I can think of to calculate an approximate to the session length
    .with_columns(
        pl.when(pl.col("exceeds"))
        .then(0)
        .otherwise(pl.col("played_delta_s"))
        .alias("played_delta_s")
    )
    .drop("exceeds")
)

st.markdown(
    """
    En "lyttesession" er en periode hvor jeg spiller musikk sammenhengende over
    en lenger periode. Hvis mellomrommet mellom to låter overgår en viss tid, la 
    oss si 20 minutter, så starter vi en ny "session".

    For at det skal kategoriseres som en session må det avspilles mer enn én
    sang.
    """
)
df_session_stats = ( 
    df_sessions 
    .group_by(pl.col("nth_session"))
    .agg(
        session_mean_s = pl.col("played_delta_s").mean(),
        tracks_played = pl.col("played_delta_s").count(),
        session_start = pl.col("track_played_utc").min(),
        session_end = pl.col("track_played_utc").max(),
    )
    .with_columns(
        session_length = (pl.col("session_end") - pl.col("session_start")),# .dt.total_seconds()
        date_played_utc = pl.col("session_end").dt.date()

    )
    # .filter(pl.col("session_length") > pl.duration(hours=3))
    .sort(pl.col("session_length"), descending=True)
)
st.dataframe(df_sessions)





# --- 
st.header("Artister og låter")

df_artists = (
    df
    .group_by(pl.col("artist_name"), pl.col("track_name"))
    .agg(
      track_scrobbles = pl.len(), # scrobbles per track
    )
    .with_columns(
      artist_scrobbles=pl.col("track_scrobbles").sum().over("artist_name") # total artist scrobbles
    )
    .sort(pl.col("artist_scrobbles"), descending=True)
)

# Get top 25 artists w.r.t. artist scrobbles
top_n = 25
top_n_artists = df_artists.select("artist_name").unique(maintain_order=True).head(top_n)
df_top_n_artists = ( 
    df_artists
    .join(top_n_artists, on="artist_name", how="semi") 
    .sort("track_scrobbles", descending=True)
)

fig_artists = px.bar(
    df_top_n_artists, 
    y="artist_name", 
    x="track_scrobbles", 
    color="track_scrobbles",
    color_continuous_scale="Emrld",
    hover_data=["artist_scrobbles", "track_name", "track_scrobbles"],
    title=f"Mest lyttede artister + låtfordeling {year}"
)
fig_artists.update_yaxes(
    categoryorder="total descending",
    autorange="reversed"
)
fig_artists.update_traces(textfont_size=10, textangle=0, textposition="outside", cliponaxis=False)
fig_artists.update_yaxes(autorange="reversed", dtick=1, title="artist")
fig_artists.update_xaxes(title="antall scrobbles")

st.plotly_chart(fig_artists)
st.caption(
    """
    Hver stolpe er satt sammen av artistens sine låter stablet oppå hverandre i
    form av klosser. Hver kloss  representerer én individuell låt, hvor 
    klossens lengde og farge bestemmes av hvor mange ganger en låt har blitt spilt 
    av. Stolpenes klosser er sortert etter hvor hyppig en låt er spilt, fra mest 
    hørt på til venstre til minst hørt på til høyre.
    """
)

year_prev = year-1
df_top_n_now = artist_ranking(df, year=year).head(top_n)
df_top_n_prev = artist_ranking(df_raw, year=year_prev)

toggle_ranking_table = st.toggle(
    "_Vis tabell over artistrangering med endringer fra året før_"
)
if toggle_ranking_table: 
    st.dataframe(
        df_top_n_now.join(df_top_n_prev, on="artist_name", how="left")
        .with_columns(
            rank_delta=pl.col(f"rank_{year_prev}") - pl.col(f"rank_{year}")
        )
        .drop(f"n_scrobbles_{year_prev}", f"n_scrobbles_{year}")
        .select(
            "artist_name", 
            f"rank_{year}", 
            f"rank_{year_prev}", 
            "rank_delta"
        ),
        height=table_height
    )
    st.caption("Tabellen viser artistenes rangering fra 2025 og 2024.")
st.markdown(
    """
    _Bladee_ har igjen blitt min mest lyttede artist i 2025 og tar tilbake plassen etter
    å ha blitt avtronet av Snow Strippers året før. _Dean Blunt_ og _Bassvictim_
    følger etter og utgjør min topp tre med med stor margin.

    _Mark William Lewis_ gjør det største hoppet i rangeringen med hele 788
    plasser, etterfulgt av _Skrillex_ og _Dean Blunt_. Det største fallet er _Snow 
    Strippers_ som går ned 12 rangeringer fra året før. _Ulla_, _MJ Lenderman_, 
    _Cameron Winter_ og _Elias Rønnenfelt_ er årets nykommere som slår gjennom i 
    rangeringen.

    Øya-relevante artister som _Bladee_, _Mk.gee_, _Charli xcx_, _MJ Lenderman_ og _Yung
    Lean_ viser seg fram her. Og det samme gjør andre artister jeg har sett live
    dette året i _Bassvictim_, _Astrid Sonne_, _Porter Robinson_, _Mark William Lewis_
    og _Elias Rønnenfelt_. Det er tydelig at artister som jeg skulle se live 
    markerr seg på rangeringen da de utgjør to femtedeler av den. 
    _Bladee_, _Charli xcx_, _Mk.gee_ og _Yung Lean_ befinner seg fremdeles høyt 
    oppe fra året før til tross for å tilhøre live-seleksjonen i år.
    """
)

# --- Artist progression plot
top_5_artists = ( 
    df_top_n_artists
    .select("artist_name", "artist_scrobbles")
    .unique("artist_name")
    .sort("artist_scrobbles", descending=True)
    .head(5)
)

df_progression = (
    df
    .join(top_5_artists, on="artist_name", how="semi")
    .group_by(
        pl.col("artist_name"), 
        pl.col("track_played_utc").dt.week().alias("week")
    )
    .agg(scrobbles = pl.len())
    .sort("week", descending=False)
)
artists = df_progression.select("artist_name").unique()
months = df_progression.select("week").unique().sort("week")

# Ensures that every artist has an entry for every week, making it easier to plot
df_progression = (
    artists.join(months, how="cross")
    .join(df_progression, on=["artist_name", "week"], how="left")
    .fill_null(0)
    .with_columns(
        cumulative_scrobbles=pl.col("scrobbles").cum_sum().over("artist_name")
    ) # calculate cumulative scrobbling for each artist
)

fig_artist_progression = px.line(
    df_progression, x="week", y="cumulative_scrobbles", color="artist_name",
    title="Kappløpet om den mest lyttede artisten",
    hover_data=["week", "cumulative_scrobbles", "artist_name", "scrobbles"]
)
fig_artist_progression.update_yaxes(title="kumulativ scrobbles")
fig_artist_progression.update_xaxes(title="uke")
st.plotly_chart(fig_artist_progression)
st.caption("Plottet viser den samlede lyttingen av artistene for hver uke.")
st.markdown(
    """
    Det er jevnt mellom _Astrid Sonne_, _Bladee_ og _Vegyn_ fram til uke 31, 
    men _Bladee_ gjør et stort byks i den perioden og overgår de to andre. 
    Dette bykset skyldes nok av at jeg skulle se _Bladee_ på Øyafestivalen i 
    uke 32, rett og slett for å gjøre meg klar til en lenge ventet konsert! 
    _Astrid Sonne_ gjør et lignende hopp fra uke 18 til uke 20, hvor jeg så 
    henne live i uke 20.

    Stigningene til _Dean Blunt_ og _Bassvictim_ kommer av at jeg skulle se dem live
    i løpet av den perioden (jeg så ikke _Dean Blunt_ live ettersom det er en
    [svært sjelden begivenhet](https://www.setlist.fm/setlists/dean-blunt-1bd109e4.html), 
    men han har et [album sammen med _Elias Rønnenfelt_](https://open.spotify.com/album/76qQt7n5SKtIa38BmZxvl4?si=GvNWtJ7qRU6pzxAggpYfLg) 
    som jeg faktisk så live i oktober).

    Igjen ser vi live-effekten drive 
    """
)

# --- top tracks
top_n_tracks = (
    df_artists.select(
        pl.col("artist_name"), pl.col("track_name"), pl.col("track_scrobbles")
    )
    .sort(pl.col("track_scrobbles"), descending=True)
    .with_columns(
        full_track_name=pl.col("artist_name") + pl.lit(" | ") + pl.col("track_name")
    )
    .head(top_n)
)

fig_tracks = px.bar(
    top_n_tracks,
    y="full_track_name",
    x="track_scrobbles",
    color="track_scrobbles",
    color_continuous_scale="Emrld",
    hover_data=["artist_name", "track_name", "track_scrobbles"],
    title=f"Mest lyttede låter {year}",
    text_auto=".0f",
)
fig_tracks.update_coloraxes(showscale=False)
fig_tracks.update_yaxes(
    categoryorder="total descending", autorange="reversed", dtick=1, title="låter"
)
fig_tracks.update_traces(
    textfont_size=20, textangle=0, textposition="outside", cliponaxis=False
)
fig_tracks.update_xaxes(title="antall scrobbles")

st.plotly_chart(fig_tracks, height=600)
st.markdown(
    """
    I stolpediagrammet ovenfor her har vi en tydeligere oversikt over hvilke låter 
    som har blitt spilt av mest. _Dean Blunt_ sin "5" skiller seg mest ut som min mest 
    spilte låt, hvor mellomrommene i avspillinger blir smalere.

    Topp 25-en min er nokså variert hvor det kun er _Dean Blunt_ og _Bladee_ som har mer
    enn én sang i rangeringen, hvor begge har to låter. (_Dataen tar ikke hensyn til 
    om en artist er en gjesteartist i en annen låt. Her teller vi kun de som er 
    hoveddartister._)
    """
)


st.subheader("Årets one-hit wonders")

df_one_hit = (
    df.group_by(pl.col("artist_name"))
    .agg(
        tracks=pl.col("track_name").unique().first(),
        track_scrobbles=pl.len(),
        unique_tracks=pl.col("track_name").unique().len(),
    )
    .filter(pl.col("unique_tracks") == 1)
    .filter(pl.col("track_scrobbles") > 2)
    .sort("track_scrobbles", descending=True)
    .drop("unique_tracks")
    .head(5)
)

st.dataframe(df_one_hit)
st.markdown(
    """
    One-hit wonder i denne kontekst følger ikke [definisjonen](https://en.wikipedia.org/wiki/One-hit_wonder) 
    regelrett på noe vis. En one-hit wonder er etter _mitt kriterium_ en artist
    som jeg har kun har spilt én låt av i løpet av året.

    Og med kriteriet i grunn går årets one-hit wonder til... _Coined_! Og _Coined_ er i denne forstand en
    one-hit wonder for de har nemlig bare [**én låt**](https://open.spotify.com/artist/0au9S2IIAu2bbXGbKfQ7Tc)
    så da har jeg nesten ikke noe valg (_Astrid Sonne_ og _Fine_ vær så snill lag
    mer musikk under _Coined_!!!).
    
    "Opus3" av _dapurr_ og "bf847" av _march_ er faktisk låter som tilhører
    henholdsvis artistene _The Hellp_ og _2hollis_ som jeg har hørt på i løpet av 
    året, hvor begge artistene dukker opp på låtrangeringen. Låtene er
    uofisielle utgivelser.
    """
)

st.header("Interaktiv seksjon")
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
monthly_top_n = 25

month_select = st.selectbox(
    "Måned",
    months_dict,
    index=0,
    width=200,
    format_func=lambda x: str(months_dict.get(x)),
)
df_monthly = df.filter(pl.col("track_played_utc").dt.month() == month_select)
df_monthly_prev = ( 
    df.filter(pl.col("track_played_utc").dt.month() == month_select-1) 
    if month_select > 1 else None
)

st.markdown(
    """
    **_TODO_**: **Tekst hvor det står 'I løpet av _måned_ hørte du på mest musikk
    _sett inn dato her_. Din mest lyttede låt den dagen var _låttittel_ av
    _artist_ som du spilte hele _antall ganger_.'**
    """
)

def monthly_stats(df):
    return (
        df
        .group_by("artist_name", "track_name")
        .agg(track_scrobbles = pl.len())
        .with_columns(
          artist_scrobbles=pl.col("track_scrobbles").sum().over("artist_name")
        )
        .sort(pl.col("track_scrobbles"), descending=True)
    )

def get_monthly_metrics(df):
    if df is None:
        return {"n_tracks": 0, "unique_tracks": 0, "unique_artists": 0}
    
    stats = monthly_stats(df)
    return {
        "n_tracks": stats["track_scrobbles"].sum(),
        "unique_tracks": stats["track_name"].n_unique(),
        "unique_artists": stats["artist_name"].n_unique(),
    }

current_month = get_monthly_metrics(df_monthly)
previous_month = get_monthly_metrics(df_monthly_prev)

monthly_metric_cols = st.columns(3)
with monthly_metric_cols[0]:
    st.metric(
        "antall låter avspilt",
        current_month["n_tracks"],
        delta=current_month["n_tracks"] - previous_month["n_tracks"],
    )
with monthly_metric_cols[1]:
    st.metric(
        "unike låter avspilt",
        current_month["unique_tracks"],
        delta=current_month["unique_tracks"] - previous_month["unique_tracks"],
    )
with monthly_metric_cols[2]:
    st.metric(
        "unike artister avspilt",
        current_month["unique_artists"],
        delta=current_month["unique_artists"] - previous_month["unique_artists"],
    )

df_monthly_artists = monthly_stats(df_monthly)
monthly_top_n_artists = (
    df_monthly_artists
    .sort(pl.col("artist_scrobbles"), descending=True)
    .select("artist_name")
    .unique(maintain_order=True)
    .head(monthly_top_n)
)
unique_monthly_artists = df_monthly_artists.select(pl.col("artist_name").unique())
unique_monthly_tracks = df_monthly_artists.select(pl.col("track_name").unique())

monthly_top_track = df_monthly_artists.row(0, named=True)
monthly_top_artist = monthly_top_n_artists.row(1)
st.metric("månedens sang på hjernen", 
          f"'{monthly_top_track["track_name"]}' av {monthly_top_track["artist_name"]}"
          )
most_repeated_a_day_monthly = (
    df_monthly
    .group_by(
        pl.col("track_played_utc").dt.date().alias("date"),
        pl.col("track_name"),
        pl.col("artist_name")
    )
    .agg(n_scrobbles = pl.len())
    .sort("n_scrobbles", descending=True)
    .row(0, named=True)
)

most_repeated_a_day = "'{}' av {} ble spilt av {} ganger den {}".format(
    most_repeated_a_day_monthly.get("track_name"),
    most_repeated_a_day_monthly.get("artist_name"),
    most_repeated_a_day_monthly.get("n_scrobbles"),
    most_repeated_a_day_monthly.get("date"),
)
st.metric("mest repeterte låt på en dag", most_repeated_a_day)

monthly_table_1, monthly_table_2 = st.columns([0.6, 0.4])
with monthly_table_1:
    st.markdown(
        f"**låtene spilt av i {months_dict.get(month_select)}**"
    )
    st.dataframe(df_monthly_artists.drop("artist_scrobbles"), height=table_height)
with monthly_table_2:
    st.markdown(
        f"**de mest avspilte artistsene i {months_dict.get(month_select)}**"
    )
    st.dataframe(
        df_monthly_artists.group_by("artist_name")
        .agg(artist_scrobbles=pl.col("artist_scrobbles").first())
        .sort("artist_scrobbles", descending=True),
        height=table_height
    )

st.divider()

df_top_n_monthly_artists = df_monthly_artists.join(
    monthly_top_n_artists, on="artist_name", how="semi"
).sort("track_scrobbles", descending=True)

fig_monthly_artists = px.bar(
    df_top_n_monthly_artists, 
    y="artist_name", 
    x="track_scrobbles", 
    color="track_scrobbles",
    color_continuous_scale="Emrld",
    hover_data=["artist_scrobbles", "track_name", "track_scrobbles"],
    title=f"Mest lyttede artister + låtfordeling – {months_dict.get(month_select)}"
)
fig_monthly_artists.update_yaxes(
    categoryorder="total descending",
    autorange="reversed"
)
fig_monthly_artists.update_traces(
    textfont_size=10, textangle=0, textposition="outside", cliponaxis=False
)
fig_monthly_artists.update_yaxes(autorange="reversed", dtick=1, title="artist")
fig_monthly_artists.update_xaxes(title="antall scrobbles")
st.plotly_chart(fig_monthly_artists)
