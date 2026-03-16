from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import requests

OMDB_API_KEY = "<YOUR OMDB KEY>"

def get_movie_data(title):
    print(f"Processing: {title}")
    url = f"http://www.omdbapi.com/?apikey={OMDB_API_KEY}&t={title}"
    r = requests.get(url)
    print(r)
    if r.status_code == 200:
        data = r.json()
        if data.get("Response") == "True":
            return {
                "title_original": title,
                "title": data.get("Title"),
                "genre": data.get("Genre"),
                "director": data.get("Director"),
                "imdb_rating": data.get("imdbRating"),
                "runtime": data.get("Runtime"),
                "poster": data.get("Poster")
            }
    return None

df_revenue = pd.read_csv("revenues_per_day.csv")
df_revenue["date"] = pd.to_datetime(df_revenue["date"])

unique_titles = df_revenue["title"].drop_duplicates().tolist()

with ThreadPoolExecutor(max_workers=10) as executor:
    dim_movie_list = list(executor.map(get_movie_data, unique_titles))

dim_movie_list = [r for r in dim_movie_list if r is not None]

dim_movie = pd.DataFrame(dim_movie_list)
dim_movie["movie_key"] = range(1, len(dim_movie)+1)

fact = df_revenue.merge(dim_movie, left_on="title", right_on="title_original", how="left")
fact = fact[["movie_key", "date", "revenue"]]
fact = fact.dropna(subset=["movie_key"])
fact["movie_key"] = fact["movie_key"].astype("Int64")


dim_date = pd.DataFrame()
dim_date["date"] = pd.to_datetime(fact["date"].unique())
dim_date["date_key"] = range(1, len(dim_date)+1)
dim_date["year"] = dim_date["date"].dt.year
dim_date["month"] = dim_date["date"].dt.month
dim_date["day"] = dim_date["date"].dt.day

fact = fact.merge(dim_date, on="date", how="left")
fact = fact[["movie_key", "date_key", "revenue"]]

fact.to_csv("fact_box_office.csv", index=False)
dim_movie.to_csv("dim_movie.csv", index=False)
dim_date.to_csv("dim_date.csv", index=False)
