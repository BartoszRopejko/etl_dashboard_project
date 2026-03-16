import pandas as pd
import snowflake.connector

conn = snowflake.connector.connect(
    user="<YOUR SNOWFLAKE USERNAME>",
    password="<YOUR SNOWFLAKE PASSWORD>",
    account="<YOUR SNOWFLAKE ACCOUNT>",
    warehouse="<YOUR SNOWFLAKE WAREHOUSE NAME>",
    database="<YOUR SNOWFLAKE DATABASE NAME>",
    schema="<YOUR SNOWFLAKE SCHEMA NAME>",
    role="<YOUR SNOWFLAKE ROLE NAME>"
)
cs = conn.cursor()
batch_size = 500

def insert_batch(df, insert_sql):
    df = df.where(pd.notnull(df), None)
    rows = [tuple(row) for row in df.itertuples(index=False, name=None)]
    for i in range(0, len(rows), batch_size):
        cs.executemany(insert_sql, rows[i:i+batch_size])

df_dim_movies = pd.read_csv("dim_movie.csv")

df_dim_movies["movie_key"] = df_dim_movies["movie_key"].astype(int)
df_dim_movies["runtime"] = df_dim_movies["runtime"].str.replace(" min", "", regex=True).fillna(0).astype(int)
df_dim_movies["imdb_rating"] = pd.to_numeric(df_dim_movies["imdb_rating"], errors="coerce").fillna(0.0)
for col in ["title", "title_original", "genre", "director", "poster"]:
    df_dim_movies[col] = df_dim_movies[col].fillna("").astype(str)

cs.execute("""
CREATE OR REPLACE TABLE dim_movies (
    movie_key INT,
    title STRING,
    genre STRING,
    director STRING,
    runtime INT,
    imdb_rating FLOAT,
    poster STRING
)
""")

insert_batch(
    df_dim_movies[["movie_key", "title", "genre", "director", "runtime", "imdb_rating", "poster"]],
    "INSERT INTO dim_movies (movie_key, title, genre, director, runtime, imdb_rating, poster) VALUES (%s, %s, %s, %s, %s, %s, %s)"
)

df_dim_date = pd.read_csv("dim_date.csv")
for col in ["date_key", "year", "month", "day"]:
    df_dim_date[col] = df_dim_date[col].astype(int)

cs.execute("""
CREATE OR REPLACE TABLE dim_date (
    date_key INT,
    year INT,
    month INT,
    day INT
)
""")

insert_batch(
    df_dim_date[["date_key", "year", "month", "day"]],
    "INSERT INTO dim_date (date_key, year, month, day) VALUES (%s, %s, %s, %s)"
)

df_fact_box_office = pd.read_csv("fact_box_office.csv")
df_fact_box_office["movie_key"] = df_fact_box_office["movie_key"].fillna(0).astype(int)
df_fact_box_office["date_key"] = df_fact_box_office["date_key"].astype(int)
df_fact_box_office["revenue"] = pd.to_numeric(df_fact_box_office["revenue"], errors="coerce").fillna(0.0)

cs.execute("""
CREATE OR REPLACE TABLE fact_box_office (
    movie_key INT,
    date_key INT,
    revenue FLOAT
)
""")

insert_batch(
    df_fact_box_office[["movie_key", "date_key", "revenue"]],
    "INSERT INTO fact_box_office (movie_key, date_key, revenue) VALUES (%s, %s, %s)"
)

conn.commit()
cs.close()
conn.close()
