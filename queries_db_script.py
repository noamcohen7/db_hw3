import mysql.connector

connection = mysql.connector.connect(
        host="127.0.0.1",
        port="3305",
        user="noamcohen7",
        password="noam77041",
        database="noamcohen7"
    )

cursor = connection.cursor()


def query_1():
    """
    Search movies using a full-text search on title and description.
    """
    try:
        cursor.execute("""
            SELECT *
            FROM movies
            WHERE MATCH(title, description) AGAINST('"action hero"' IN BOOLEAN MODE)
        """)
        output = cursor.fetchall()
        return output
    except mysql.connector.Error as err:
        print(f"Error searching movies: {err}")
        return []

def query_2():
    """
    Executes query number 2: Find actors with 'Oscar winner' in their biography.
    """
    try:
        cursor.execute("""
            SELECT *
            FROM actors
            WHERE MATCH(biography) AGAINST('"Oscar winner"' IN BOOLEAN MODE)
        """)
        results = cursor.fetchall()
        return results
    except mysql.connector.Error as err:
        print(f"Error executing query 2: {err}")
        return []


def query_3():
    """ Executes query number 3 """
    try:
        cursor.execute(""" 
        SELECT g.genre_name, COUNT(*) AS movie_count
        FROM genres g
        JOIN movies m ON g.id = m.genre_id
        GROUP BY g.genre_name
        HAVING COUNT(*) = (
            SELECT MAX(movie_count)
            FROM (
                SELECT g.genre_name, COUNT(*) AS movie_count
                FROM genres g
                JOIN movies m ON g.id = m.genre_id
                GROUP BY g.genre_name
            ) AS genre_counts
        )""")
        results = cursor.fetchall()
        return results
    except mysql.connector.Error as err:
        print(f"Error executing query 3: {err}")
        return []

def query_4():
    """ Executes query number 4 """
    """
        Executes query: Find actor pairs who have acted in more than one movie together.
        """
    try:
        cursor.execute("""
                SELECT 
                    a1.actor_name AS actor1,
                    a2.actor_name AS actor2,
                    COUNT(*) AS movie_count
                FROM film_actor fa1
                JOIN film_actor fa2 ON fa1.movie_id = fa2.movie_id AND fa1.actor_id < fa2.actor_id
                JOIN actors a1 ON fa1.actor_id = a1.id
                JOIN actors a2 ON fa2.actor_id = a2.id
                GROUP BY fa1.actor_id, fa2.actor_id
                HAVING movie_count > 1
            """)
        results = cursor.fetchall()
        return results
    except mysql.connector.Error as err:
        print(f"Error executing query_shared_movies: {err}")
        return []


def query_5():
    """
    Executes query number 5: Find directors who directed movies in at least 3 different genres.
    """
    try:
        cursor.execute("""
            SELECT 
                directors.director_name, 
                COUNT(DISTINCT genres.genre_name) AS genre_count
            FROM movies
            JOIN genres ON genres.id = movies.genre_id
            JOIN directors on directors.id = movies.director_id
            GROUP BY directors.director_name
            HAVING genre_count >= 3
        """)
        results = cursor.fetchall()
        return results
    except mysql.connector.Error as err:
        print(f"Error executing query 5: {err}")
        return []



def main():
    query_1()
    query_2()
    query_3()
    query_4()
    query_5()

if __name__ == '__main__':
    main()