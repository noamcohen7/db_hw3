import mysql.connector

def create_tables():
    """
    Creates the tables scheme
    If table exists than table will not be recreated
    """
    connection = mysql.connector.connect(
        host="127.0.0.1",
        port="3305",
        user="noamcohen7",
        password="noam77041",
        database="noamcohen7"
    )
    cursor = connection.cursor()

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

    # Create Actors Table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS actors (
        id INT AUTO_INCREMENT PRIMARY KEY,
        actor_name VARCHAR(255) NOT NULL,
        biography TEXT,
        FULLTEXT(biography)
    )""")

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

    # Create Customers Table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS customers (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            email VARCHAR(255) UNIQUE NOT NULL,
            country VARCHAR(100) NOT NULL,
            INDEX (country)
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

def delete_tables():
    """
    Responsible for deleting the tables
    To be used only for work progress
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

    # Drop tables in reverse order to avoid foreign key conflicts
    cursor.execute("DROP TABLE IF EXISTS film_actor")
    cursor.execute("DROP TABLE IF EXISTS rentals")
    cursor.execute("DROP TABLE IF EXISTS customers")
    cursor.execute("DROP TABLE IF EXISTS movies")
    cursor.execute("DROP TABLE IF EXISTS actors")
    cursor.execute("DROP TABLE IF EXISTS genres")
    cursor.execute("DROP TABLE IF EXISTS directors")

    connection.commit()
    cursor.close()
    connection.close()

if __name__ == "__main__":

    create_tables()
    # delete_tables() use - this function only if you want to check the creation of db from scratch
