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

def insert_movies(cursor, movies):
    """
    Insert movies into the database.
    """
    for movie in movies:
        try:
            cursor.execute("""
                INSERT INTO movies (title, description, genre_id, director_id)
                VALUES (%s, %s,
                        (SELECT id FROM genres WHERE genre_name = %s),
                        (SELECT id FROM directors WHERE director_name = %s))
            """, (movie['title'], movie['description'], movie['genre'], movie['director']))
        except mysql.connector.Error as err:
            print(f"Error inserting movie {movie['title']}: {err}")

def insert_customers(cursor, customers):
    """
    Insert customers into the database.
    """
    for customer in customers:
        try:
            cursor.execute("""
                INSERT IGNORE INTO customers (name, email, country)
                VALUES (%s, %s, %s)
            """, (customer['name'], customer['email'], customer['country']))
        except mysql.connector.Error as err:
            print(f"Error inserting customer {customer['name']}: {err}")

def insert_rentals(cursor, rentals):
    """
    Insert rentals into the database.
    """
    for rental in rentals:
        try:
            cursor.execute("""
                INSERT INTO rentals (customer_id, movie_id, rental_date)
                VALUES (
                    (SELECT id FROM customers WHERE email = %s),
                    (SELECT id FROM movies WHERE title = %s),
                    %s
                )
            """, (rental['customer_email'], rental['movie_title'], rental['rental_date']))
        except mysql.connector.Error as err:
            print(f"Error inserting rental for customer {rental['customer_email']}: {err}")

def insert_film_actor(cursor, film_actors):
    """
    Insert film-actor relationships into the database.
    """
    for film_actor in film_actors:
        try:
            cursor.execute("""
                INSERT IGNORE INTO film_actor (movie_id, actor_id)
                VALUES (
                    (SELECT id FROM movies WHERE title = %s),
                    (SELECT id FROM actors WHERE actor_name = %s)
                )
            """, (film_actor['movie_title'], film_actor['actor_name']))
        except mysql.connector.Error as err:
            print(f"Error inserting film_actor for movie {film_actor['movie_title']}: {err}")

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

if __name__ == "__main__":
    populate_database()
