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

year = 2025
df_raw = st.session_state["scrobble_data"]
df = df_raw.filter(pl.col("track_played_utc").dt.year() == year)

# ---
st.write("ToC")

st.markdown("_Dette er på ingen måte en blodseriøs analyse._")

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

# ---
st.subheader("2025 sammenlignet årene før")
st.write(df_pct_change)
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
st.subheader("Lyttetrend utover 2025")
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

st.markdown(
    "**TODO: Sett rekkefølgen til å være månedlig -> ukedager -> daglig -> timesvis**"
)

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

# df_daily_hist = (df
#     .group_by(pl.col("track_played_utc").dt.weekday().alias("weekday_idx"))
#     .agg(scrobbles_by_weekday = pl.len())
#     .sort("scrobbles_by_weekday", descending=True)
#     .with_columns(weekday = pl.col("weekday_idx").replace_strict(weekdays, default=None).alias("weekday"))
# )
#
# fig_daily_hist = px.histogram(
#     df_daily_hist, 
#     x="weekday", 
#     y="scrobbles_by_weekday", 
#     nbins=7, 
#     text_auto=".0f",
#     category_orders={"weekday": list(weekdays.values())},
#     title="Frekvens av scrobbles i ukedagene"
# )
# fig_daily_hist.update_layout(
#     bargap=0.2, xaxis=dict(title="ukedag"), yaxis=dict(title="antall scrobbles")
# )
# fig_daily_hist.update_traces(
#     textfont_size=10, textangle=0, textposition="outside", cliponaxis=False
# )
# st.plotly_chart(fig_daily_hist)


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
    title="Frekvens av scrobbles i løpet av en dag"
    )
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
Når vi plotter histogrammet av frekvensen ser vi at fordelingen er skjev med 
topp på venstresiden og en hale mot høyre. Vi ser at det er av det sjeldnere 
slag at jeg lytter gjennom 70 låter på én dag.
"""
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
    bargap=0.2, xaxis=dict(title="time"), yaxis=dict(title="antall scrobbles")
)
fig_hour_hist.update_traces(
    textfont_size=10, textangle=0, textposition="outside", cliponaxis=False
)

st.plotly_chart(fig_hour_hist)
st.markdown(
"""
Ut ifra den fordelingen av døgnets timer er det klart at mesteparten av 
lyttingen foregår i nitiden fram firetiden, etterfulgt av en periode fra fire 
til syv på ettermiddagen; vi har en bimodal fordeling hvor det er størst 
aktivitet på formiddagen etterfulgt av en mindre aktiv periode på 
ettermiddagstid.
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
    textfont_size=10, textangle=0, textposition="outside", cliponaxis=False
)
st.plotly_chart(fig_daily_hist)
st.markdown(
"""
Av hverdagene (man-fre) er det tirsdag som ikke helt er på samme nivå som 
resten. Lørdag og søndag derimot ser vi en sterk nedgang i musikklytting.
"""
)

st.subheader("Sammenhengende lytting")
st.write(listening_streak(df, year))
st.markdown(
"""
Dette er for meg overraskende. At jeg har i 212 sammenhengende
dager åpnet opp Spotify-appen og spilt en låt er både urovekkende og imponerende. 
Men at det kun er totalt 2 dager av året hvor jeg ikke har åpnet opp appen er enda 
mer urovekkende.

For å sette dette i perspektiv kan vi se på heatmappet under:
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
    "Dette ser mye bedre ut enn [min GitHub contribution graph](https://github.com/aadle)."
)


# --- 
st.subheader("Artister og låter som utgjorde året")

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
fig_artists.update_yaxes(autorange="reversed", dtick=1)

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
    )
)
st.caption("Tabellen viser artistenes rangering fra 2025 og 2024.")
st.markdown(
    """
    Bladee er igjen min mest lyttede artist i 2025 og tar tilbake plassen etter
    å ha blitt avtronet av Snow Strippers året før. Dean Blunt og Bassvictim
    følger etter og utgjør min topp tre med med stor margin.

    Mark William Lewis gjør det største hoppet i rangeringen med hele 788
    plasser, etterfulgt av Skrillex og Dean Blunt. Det største fallet er Snow 
    Strippers som går ned 12 rangeringer fra året før. Ulla, MJ Lenderman, 
    Cameron Winter og Elias Rønnenfelt er årets nykommere som slår gjennom i 
    rangeringen.

    Øya-relevante artister som Bladee, Mk.gee, Charli xcx, MJ Lenderman og Yung
    Lean viser seg fram her. Og det samme gjør andre artister jeg har sett live
    dette året i Bassvictim, Astrid Sonne, Porter Robinson, Mark William Lewis
    og Elias Rønnenfelt. Det er tydelig at artister som jeg skulle se live tar 
    sin plass på rangeringen da de utgjør to femtedeler av den. Bladee, Charli 
    xcx, Mk.gee og Yung Lean befinner seg fremdeles høyt oppe fra året før til 
    tross for å tilhøre live-seleksjonen i år.
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
    Det er jevnt mellom Astrid Sonne, Bladee og Vegyn opp til uke 31, men Bladee
    gjør et stort byks i den perioden og overgår de to andre. Dette bykset
    skyldes nok av at jeg skulle se Bladee på Øyafestivalen i uke 32, rett og
    slett for å hype meg opp til god konsert! Astrid Sonne gjør et lignende hopp
    fra uke 18 til uke 20, hvor jeg så henne live i uke 20.

    Stigningene til Dean Blunt og Bassvictim kommer av at jeg skulle se dem live
    i løpet av den perioden (jeg så ikke Dean Blunt live ettersom det er en
    [svært sjelden begivenhet](https://www.setlist.fm/setlists/dean-blunt-1bd109e4.html), 
    men han har et [album sammen med Elias Rønnenfelt](https://open.spotify.com/album/76qQt7n5SKtIa38BmZxvl4?si=GvNWtJ7qRU6pzxAggpYfLg) 
    som jeg faktisk så live i oktober).
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
    hover_data=["artist_name", "track_name", "track_scrobbles"],
    title=f"Mest lyttede låter {year}",
    text_auto=".0f",
)
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
som har blitt spilt av mest. Dean Blunt sin "5" skiller seg mest ut som min mest 
spilte låt, hvor mellomrommene i avspillinger blir smalere.

Topp 25-en min er nokså variert hvor det kun er Dean Blunt og Bladee som har mer
enn én sang i rangeringen, hvor begge har to låter. (_Dataen tar ikke hensyn til 
om en artist er en gjesteartist i en annen låt. Her teller vi kun de som er 
hoveddartister._)
"""
)

st.markdown(
"""
_Oppsummering som viser topp n artister, topp 3 sanger (hvis mulig), og antall
scrobbles. Kanskje i én egen side._
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

    Med kriteriet i grunn går årets one-hit wonder til... Coined! Og Coined er i denne forstand en
    one-hit wonder for de har nemlig bare [**én låt**](https://open.spotify.com/artist/0au9S2IIAu2bbXGbKfQ7Tc)
    så da har jeg nesten ikke noe valg (Astrid Sonne og Fine vær så snill lag
    mer musikk under Coined!!!).
    
    "Opus3" av dapurr og "bf847" av march er faktisk låter som tilhører
    henholdsvis artistene The Hellp og 2hollis som jeg har hørt på i løpet av 
    året, hvor begge artistene dukker opp på låtrangeringen.
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
    "Måned", months_dict, index=0, width=200, format_func=lambda x: str(months_dict.get(x))
)
df_monthly = df.filter(pl.col("track_played_utc").dt.month() == month_select)

st.markdown(
    """
    **_TODO_**: **Tekst hvor det står 'I løpet av _måned_ hørte du på mest musikk
    _sett inn dato her_. Din mest lyttede låt den dagen var _låttittel_ av
    _artist_ som du spilte hele _antall ganger_.'**
    """
)

monthly_table_1, monthly_table_2 = st.columns([0.6, 0.4])
with monthly_table_1:
    st.markdown(f"**De {monthly_top_n} mest avspilte låtene i {months_dict.get(month_select)}**")
    st.dataframe(
        df_monthly
        .group_by("artist_name", "track_name")
        .agg(track_scrobbles = pl.len())
        .sort(pl.col("track_scrobbles"), descending=True)
        .head(monthly_top_n)
    )
with monthly_table_2:
    st.markdown(f"**De {monthly_top_n} mest avspilte artistsene i {months_dict.get(month_select)}**")
    st.dataframe(
        df_monthly
        .group_by("artist_name")
        .agg(artist_scrobbles = pl.len())
        .sort(pl.col("artist_scrobbles"), descending=True)
        .head(monthly_top_n)
    )

df_monthly_artists = (
    df_monthly
    .group_by("artist_name", "track_name")
    .agg(track_scrobbles = pl.len())
    .with_columns(
      artist_scrobbles=pl.col("track_scrobbles").sum().over("artist_name")
    )
    .sort(pl.col("track_scrobbles"), descending=True)
)

monthly_top_n_artists = (
    df_monthly_artists
    .sort(pl.col("artist_scrobbles"), descending=True)
    .select("artist_name")
    .unique(maintain_order=True)
    .head(monthly_top_n)
)
df_top_n_monthly_artists = df_monthly_artists.join(monthly_top_n_artists, on="artist_name", how="semi").sort(
    "track_scrobbles", descending=True
)

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
