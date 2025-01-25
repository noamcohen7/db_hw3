import mysql.connector

def connect_to_database():
    """
    Establish a connection to the MySQL database.
    """
    return mysql.connector.connect(
        host="127.0.0.1",
        port="3305",
        user="noamcohen7",
        password="noam77041",
        database="noamcohen7"
    )

def insert_genres(cursor, genres):
    """
    Insert genres into the database.
    """
    for genre in genres:
        try:
            cursor.execute("""
                INSERT IGNORE INTO genres (genre_name) VALUES (%s)
            """, (genre,))
        except mysql.connector.Error as err:
            print(f"Error inserting genre {genre}: {err}")

def update_genre(cursor, genre_id, new_genre_name):
    """
    Update a genre in the database.
    """
    try:
        cursor.execute("""
            UPDATE genres SET genre_name = %s WHERE id = %s
        """, (new_genre_name, genre_id))
    except mysql.connector.Error as err:
        print(f"Error updating genre ID {genre_id}: {err}")

def insert_directors(cursor, directors):
    """
    Insert directors into the database.
    """
    for director in directors:
        try:
            cursor.execute("""
                INSERT IGNORE INTO directors (director_name) VALUES (%s)
            """, (director,))
        except mysql.connector.Error as err:
            print(f"Error inserting director {director}: {err}")

def update_director(cursor, director_id, new_director_name):
    """
    Update a director in the database.
    """
    try:
        cursor.execute("""
            UPDATE directors SET director_name = %s WHERE id = %s
        """, (new_director_name, director_id))
    except mysql.connector.Error as err:
        print(f"Error updating director ID {director_id}: {err}")

def insert_actors(cursor, actors):
    """
    Insert actors into the database.
    """
    for actor in actors:
        try:
            cursor.execute("""
                INSERT IGNORE INTO actors (actor_name, biography) VALUES (%s, %s)
            """, (actor['name'], actor['biography']))
        except mysql.connector.Error as err:
            print(f"Error inserting actor {actor['name']}: {err}")

def update_actor(cursor, actor_id, new_name, new_biography):
    """
    Update an actor in the database.
    """
    try:
        cursor.execute("""
            UPDATE actors SET actor_name = %s, biography = %s WHERE id = %s
        """, (new_name, new_biography, actor_id))
    except mysql.connector.Error as err:
        print(f"Error updating actor ID {actor_id}: {err}")

def insert_movies(cursor, movies):
    """
    Insert movies into the database.
    """
    for movie in movies:
        try:
            cursor.execute("""
                INSERT INTO movies (title, description, genre_id, director_id)
                VALUES (%s, %s,
                        (SELECT id FROM genres WHERE genre_name = %s LIMIT 1),
                        (SELECT id FROM directors WHERE director_name = %s LIMIT 1))
            """, (movie['title'], movie['description'], movie['genre'], movie['director']))
        except mysql.connector.Error as err:
            print(f"Error inserting movie {movie['title']}: {err}")

def update_movie(cursor, movie_id, new_title, new_description):
    """
    Update a movie in the database.
    """
    try:
        cursor.execute("""
            UPDATE movies SET title = %s, description = %s WHERE id = %s
        """, (new_title, new_description, movie_id))
    except mysql.connector.Error as err:
        print(f"Error updating movie ID {movie_id}: {err}")


def insert_film_actor(cursor, film_actors):
    """
    Insert film-actor relationships into the database.
    """
    for film_actor in film_actors:
        try:
            cursor.execute("""
                INSERT IGNORE INTO film_actor (movie_id, actor_id)
                VALUES (
                    (SELECT id FROM movies WHERE title = %s LIMIT 1),
                    (SELECT id FROM actors WHERE actor_name = %s LIMIT 1)
                )
            """, (film_actor['movie_title'], film_actor['actor_name']))
        except mysql.connector.Error as err:
            print(f"Error inserting film_actor for movie {film_actor['movie_title']}: {err}")


def update_film_actor(cursor, old_movie_title, old_actor_name, new_movie_title, new_actor_name):
    """
    Update film-actor relationships when movies or actors are updated.
    """
    try:
        cursor.execute("""
            UPDATE film_actor
            SET movie_id = (SELECT id FROM movies WHERE title = %s LIMIT 1),
                actor_id = (SELECT id FROM actors WHERE actor_name = %s LIMIT 1)
            WHERE movie_id = (SELECT id FROM movies WHERE title = %s LIMIT 1)
              AND actor_id = (SELECT id FROM actors WHERE actor_name = %s LIMIT 1)
        """, (new_movie_title, new_actor_name, old_movie_title, old_actor_name))
    except mysql.connector.Error as err:
        print(f"Error updating film_actor for movie {old_movie_title} and actor {old_actor_name}: {err}")


def populate_database():
    """
    Main function to populate the database with data.
    """
    connection = connect_to_database()
    cursor = connection.cursor()

    # Example: Insert Genres
    genres = ["Action", "Comedy", "Drama", "Thriller"]
    insert_genres(cursor, genres)

    # Example: Insert Directors
    directors = ["Christopher Nolan", "Steven Spielberg", "Quentin Tarantino"]
    insert_directors(cursor, directors)

    # Example: Insert Actors
    actors = [
        {"name": "Leonardo DiCaprio", "biography": "Oscar-winning actor known for Inception and Titanic."},
        {"name": "Brad Pitt", "biography": "Versatile actor known for Fight Club and Once Upon a Time in Hollywood."}
    ]
    insert_actors(cursor, actors)

    # Example: Insert Movies
    movies = [
        {"title": "Inception", "description": "A mind-bending thriller", "genre": "Action", "director": "Christopher Nolan"},
        {"title": "Pulp Fiction", "description": "Crime drama", "genre": "Drama", "director": "Quentin Tarantino"}
    ]
    insert_movies(cursor, movies)

    # Example: Insert Customers
    customers = [
        {"name": "John Doe", "email": "john.doe@example.com", "country": "USA"},
        {"name": "Jane Smith", "email": "jane.smith@example.com", "country": "Canada"}
    ]
    insert_customers(cursor, customers)

    # Example: Insert Rentals
    rentals = [
        {"customer_email": "john.doe@example.com", "movie_title": "Inception", "rental_date": "2025-01-01"},
        {"customer_email": "jane.smith@example.com", "movie_title": "Pulp Fiction", "rental_date": "2025-01-02"}
    ]
    insert_rentals(cursor, rentals)

    # Example: Insert Film-Actor Relationships
    film_actors = [
        {"movie_title": "Inception", "actor_name": "Leonardo DiCaprio"},
        {"movie_title": "Pulp Fiction", "actor_name": "Brad Pitt"}
    ]
    insert_film_actor(cursor, film_actors)

    connection.commit()
    cursor.close()
    connection.close()

def update_the_db_tables():
    """
    Example usage of update functions.
    """
    connection = connect_to_database()
    cursor = connection.cursor()

    # Example: Update a genre
    update_genre(cursor, genre_id=1, new_genre_name="Adventure")

    # Example: Update a director
    update_director(cursor, director_id=1, new_director_name="Christopher Edward Nolan")

    # Example: Update an actor
    update_actor(cursor, actor_id=1, new_name="Leonardo Wilhelm DiCaprio", new_biography="Oscar-winning actor known for Titanic and The Revenant.")

    # Example: Update a movie
    update_movie(cursor, movie_id=1, new_title="Inception (Extended Edition)", new_description="A mind-bending thriller with extended scenes.")

    # Example: Update a customer
    update_customer(cursor, customer_id=1, new_name="Johnathan Doe", new_email="johnathan.doe@example.com", new_country="United States")

    connection.commit()
    cursor.close()
    connection.close()

if __name__ == "__main__":
    populate_database()
    update_the_db_tables()
