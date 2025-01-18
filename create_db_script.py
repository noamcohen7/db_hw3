import mysql.connector

def create_database():
    """
    Creates the db tables that
    :return:
    """
    connection = mysql.connector.connect(
        host="127.0.0.1",
        port="3305",
        user="noamcohen7",
        password="noam77041",
        database="noamcohen7"
    )
    cursor = connection.cursor()
    cursor.execute("CREATE DATABASE IF NOT EXISTS movie_db")
    cursor.close()
    connection.close()

def create_tables():
    connection = mysql.connector.connect(
        host="localhost",
        user="your_username",
        password="your_password",
        database="movie_db"
    )
    cursor = connection.cursor()

    # Create Movies Table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS movies (
        id INT AUTO_INCREMENT PRIMARY KEY,
        title VARCHAR(255) NOT NULL,
        description TEXT,
        genre_id INT,
        director_id INT,
        FULLTEXT(title, description),
        FOREIGN KEY (genre_id) REFERENCES genres(id),
        FOREIGN KEY (director_id) REFERENCES directors(id),
        INDEX (genre_id),
        INDEX (director_id)
    )""")

    # Create Actors Table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS actors (
        id INT AUTO_INCREMENT PRIMARY KEY,
        actor_name VARCHAR(255) NOT NULL,
        biography TEXT,
        FULLTEXT(biography)
    )""")

    # Create Genres Table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS genres (
        id INT AUTO_INCREMENT PRIMARY KEY,
        genre_name VARCHAR(100) UNIQUE NOT NULL
    )""")

    # Create Directors Table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS directors (
        id INT AUTO_INCREMENT PRIMARY KEY,
        director_name VARCHAR(255) NOT NULL
    )""")

    # Create Rentals Table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS rentals (
        id INT AUTO_INCREMENT PRIMARY KEY,
        customer_id INT NOT NULL,
        movie_id INT NOT NULL,
        rental_date DATE NOT NULL,
        FOREIGN KEY (customer_id) REFERENCES customers(id),
        FOREIGN KEY (movie_id) REFERENCES movies(id),
        INDEX (customer_id),
        INDEX (movie_id),
        INDEX (rental_date)
    )""")

    # Create Customers Table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS customers (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        email VARCHAR(255) UNIQUE NOT NULL,
        country VARCHAR(100) NOT NULL,
        INDEX (country)
    )""")

    # Create Film_Actor Table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS film_actor (
        id INT AUTO_INCREMENT PRIMARY KEY,
        movie_id INT NOT NULL,
        actor_id INT NOT NULL,
        UNIQUE(movie_id, actor_id),
        FOREIGN KEY (movie_id) REFERENCES movies(id),
        FOREIGN KEY (actor_id) REFERENCES actors(id),
        INDEX (movie_id),
        INDEX (actor_id)
    )""")

    connection.commit()
    cursor.close()
    connection.close()

if __name__ == "__main__":
    create_database()
    create_tables()
