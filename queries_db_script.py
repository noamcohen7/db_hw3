import mysql.connector

connection = mysql.connector.connect(
        host="127.0.0.1",
        port="3305",
        user="noamcohen7",
        password="noam77041",
        database="noamcohen7"
    )

cursor = connection.cursor()



# Create query
def query_1():
    cursor.execute("""
        select(movies).where(func.match(movies.c.title, movies.c.description).against('action hero'))""")

def query_2():
    cursor.execute("""select(actors).where(func.match(actors.c.biography).against('Oscar winner'))""")


def query_3():
    cursor.execute("""
     select(customers.c.country, genres.c.genre, func.count(1).label('genre_count'))
    .join(rentals, customers.c.customer_id == rentals.c.customer_id)
    .join(films, rentals.c.film_id == films.c.film_id)
    .join(genres, films.c.genre_id == genres.c.genre_id)
    .group_by(customers.c.country, genres.c.genre)
    .having(
        func.count(1) == (
            select(func.max(genre_count))
            .from_(
                select(customers.c.country, genres.c.genre, func.count(1).label('genre_count'))
                .join(rentals, customers.c.customer_id == rentals.c.customer_id)
                .join(films, rentals.c.film_id == films.c.film_id)
                .join(genres, films.c.genre_id == genres.c.genre_id)
                .group_by(customers.c.country, genres.c.genre)
            ).alias('genre_counts')
            .where(genre_counts.c.country == customers.c.country)
            )
        )
    )""")

def query_4():
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
    cursor.execute("""select(films.c.director_name, func.count(distinct(genres.c.genre)).label('genre_count'))
    .join(genres, genres.c.id == films.c.genre_id)
    .group_by(films.c.director_name)
    .having(func.count(distinct(genres.c.genre)) >= 3)
    )""")


def main():
    query_1()
    query_2()
    query_3()
    query_4()
    query_5()