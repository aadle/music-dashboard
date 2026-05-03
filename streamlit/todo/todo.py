import streamlit as st

st.header("Veien videre")

with st.container():
    st.subheader("data")
    st.checkbox("Integrere artist- og låtdata i appen")
    st.checkbox("Trekke fram sjanger-tags for artistene i den grad det er tilstrekkelig")

    st.subheader("'yearly overview'")
    st.checkbox("Ha metrics over sjangere (mest lyttet, minst lyttet)")
    st.checkbox("Integrere et kakediagram(?) som viser fordeling av sjangre som preger lyttingen")

    st.subheader("refactoring")
    st.checkbox("Flytte nyttige funksjoner til egen fil")
