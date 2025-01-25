import time
import requests
from api_data_retrieve import connect_to_database, insert_genres, insert_directors, insert_actors, insert_movies, insert_film_actor

TMDB_API_KEY = "3ca5e21a465130729430a0d3134183f2"
TMDB_BASE_URL = "https://api.themoviedb.org/3"

def fetch_genres_from_tmdb():
    """
    Fetch genres from TMDB API.
    """
    url = f"{TMDB_BASE_URL}/genre/movie/list"
    response = requests.get(url, params={"api_key": TMDB_API_KEY, "language": "en-US"})
    response.raise_for_status()
    genres = response.json().get("genres", [])
    return genres  # List of genre dictionaries

def fetch_actor_biography(actor_id):
    """
    Fetch biography for a specific actor using TMDB API.
    """
    url = f"{TMDB_BASE_URL}/person/{actor_id}"
    response = requests.get(url, params={"api_key": TMDB_API_KEY, "language": "en-US"})
    response.raise_for_status()
    actor_data = response.json()
    return actor_data.get("biography", "Biography unavailable")

def fetch_popular_movies_from_tmdb(pages=5000):
    """
    Fetch popular movies from TMDB API across multiple pages.
    """
    all_movies = []
    for page in range(1, pages + 1):
        url = f"{TMDB_BASE_URL}/movie/popular"
        response = requests.get(url, params={"api_key": TMDB_API_KEY, "language": "en-US", "page": page})
        if response.status_code == 429:
            print("Rate limit exceeded. Waiting before retrying...")
            time.sleep(10)  # Wait before retrying
            continue
        response.raise_for_status()
        movies = response.json().get("results", [])
        all_movies.extend(movies)
    return all_movies

def fetch_movie_credits(movie_id):
    """
    Fetch credits for a specific movie (to get directors and actors).
    """
    url = f"{TMDB_BASE_URL}/movie/{movie_id}/credits"
    response = requests.get(url, params={"api_key": TMDB_API_KEY, "language": "en-US"})
    response.raise_for_status()
    return response.json()

def populate_database_from_tmdb():
    """
    Populate the database with data from TMDB API.
    """
    connection = connect_to_database()
    cursor = connection.cursor()

    # Fetch and insert genres
    genres = fetch_genres_from_tmdb()
    insert_genres(cursor, [genre["name"] for genre in genres])

    # Fetch and insert movies and related data
    movies = fetch_popular_movies_from_tmdb(pages=100)  # Adjusted for 100 pages
    for movie in movies:
        # Map genre IDs to genre names
        movie_genres = [genre["name"] for genre in genres if genre["id"] in movie["genre_ids"]]
        genre_name = movie_genres[0] if movie_genres else "Unknown"

        # Fetch credits for movie
        credits = fetch_movie_credits(movie["id"])
        directors = [crew["name"] for crew in credits["crew"] if crew["job"] == "Director"]
        actors = credits["cast"][:10]  # Limit to top 10 actors

        # Insert directors
        insert_directors(cursor, directors)

        # Insert movie
        if directors:
            insert_movies(cursor, [{
                "title": movie["title"],
                "description": movie["overview"],
                "genre": genre_name,
                "director": directors[0]
            }])

        # Insert actors and film-actor relationships
        for actor in actors:
            actor_biography = fetch_actor_biography(actor["id"])
            insert_actors(cursor, [{"name": actor["name"], "biography": actor_biography}])
            insert_film_actor(cursor, [{"movie_title": movie["title"], "actor_name": actor["name"]}])

    connection.commit()
    cursor.close()
    connection.close()

if __name__ == "__main__":
    populate_database_from_tmdb()
