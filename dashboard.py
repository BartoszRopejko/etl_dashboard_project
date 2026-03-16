import streamlit as st
import pandas as pd
import snowflake.connector
import plotly.express as px


conn = snowflake.connector.connect(
    user="<YOUR SNOWFLAKE USERNAME>",
    password="<YOUR SNOWFLAKE PASSWORD>",
    account="<YOUR SNOWFLAKE ACCOUNT>",
    warehouse="<YOUR SNOWFLAKE WAREHOUSE NAME>",
    database="<YOUR SNOWFLAKE DATABASE NAME>",
    schema="<YOUR SNOWFLAKE SCHEMA NAME>",
    role="<YOUR SNOWFLAKE ROLE>"
)
cs = conn.cursor()


@st.cache_data
def load_data():
    cs.execute("""
        SELECT f.movie_key, f.date_key, f.revenue,
               m.title, m.genre, m.director, m.runtime, m.imdb_rating, m.poster,
               d.year, d.month, d.day
        FROM fact_box_office f
        JOIN dim_movies m ON f.movie_key = m.movie_key
        JOIN dim_date d ON f.date_key = d.date_key
    """)
    df = pd.DataFrame(cs.fetchall(), columns=[
        "movie_key","date_key","revenue",
        "title","genre","director","runtime","imdb_rating","poster",
        "year","month","day"
    ])
    return df

df = load_data()

st.sidebar.header("Filters")
years = sorted(df["year"].unique())
genres = sorted(df["genre"].unique())

selected_year = st.sidebar.multiselect("Choose year", years, default=years)
selected_genre = st.sidebar.multiselect("Choose genre", genres, default=genres)

df_filtered = df[df["year"].isin(selected_year) & df["genre"].isin(selected_genre)]

st.title("Dashboard Box Office")

st.subheader("Top 10 based on revenue")
top10 = df_filtered.groupby("title")["revenue"].sum().sort_values(ascending=False).head(10).reset_index()
fig1 = px.bar(top10, x="title", y="revenue", text="revenue", labels={"revenue":"Revenue"})
st.plotly_chart(fig1, use_container_width=True)

st.subheader("Revenue trend by years")
trend = df_filtered.groupby("year")["revenue"].sum().reset_index()
fig2 = px.line(trend, x="year", y="revenue", markers=True, labels={"revenue":"Revenue"})
st.plotly_chart(fig2, use_container_width=True)


st.subheader("Movies with longest runtime")
longest_movies = df_filtered[["title","runtime","genre","year"]].sort_values("runtime", ascending=False).head(10)
st.dataframe(longest_movies)

st.subheader("Movie cards")
for idx, row in df_filtered.head(10).iterrows():
    st.markdown(f"**{row['title']} ({row['year']})**")
    st.image(row["poster"], width=150)
    st.write(f"Genre: {row['genre']}, Director: {row['director']}, Runtime: {row['runtime']} min, IMDb: {row['imdb_rating']}")
    st.markdown("---")

cs.close()
conn.close()
