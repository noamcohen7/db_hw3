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
    """ Executes query number 1 """
    cursor.execute("""
        select(movies).where(func.match(movies.c.title, movies.c.description).against('action hero'))""")

def query_2():
    """ Executes query number 2 """
    cursor.execute("""select(actors).where(func.match(actors.c.biography).against('Oscar winner'))""")


def query_3():
    """ Executes query number 3 """
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

def query_4():
    """ Executes query number 4 """
    cursor.execute("""
        select(
            actors.c.actor_name.label('actor1'),
            actors.c.actor_name.label('actor2'),
            func.count(1).label('movie_count')
        )
        .join(film_actor.alias('fa1'), film_actor.c.actor_id == actors.c.id)
        .join(film_actor.alias('fa2'), film_actor.c.movie_id == film_actor.c.movie_id, film_actor.c.actor_id < film_actor.c.actor_id)
        .join(actors, film_actor.c.actor_id == actors.c.id)
        .group_by(film_actor.c.actor_id, film_actor.c.actor_id)
        .having(func.count(1) > 1)
    )""")


def query_5():
    """ Executes query number 5 """
    cursor.execute("""select(films.c.director_name, func.count(distinct(genres.c.genre)).label('genre_count'))
    .join(genres, genres.c.id == films.c.genre_id)
    .group_by(films.c.director_name)
    .having(func.count(distinct(genres.c.genre)) >= 3)
    )""")
