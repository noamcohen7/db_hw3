Project hw3 in DBMS course
The following queries are executed against a DB designed to have the queries executed efficiently.

1.  SELECT *
    FROM movies
    WHERE MATCH(title, description) AGAINST('"action hero"' IN BOOLEAN MODE)

2.  SELECT *
    FROM actors
    WHERE MATCH(biography) AGAINST('"Oscar winner"' IN BOOLEAN MODE)

3.  SELECT g.genre_name, COUNT(*) AS movie_count
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

4.  SELECT 
        a1.actor_name AS actor1,
        a2.actor_name AS actor2,
        COUNT(*) AS movie_count
    FROM film_actor fa1
    JOIN film_actor fa2 ON fa1.movie_id = fa2.movie_id AND fa1.actor_id < fa2.actor_id
    JOIN actors a1 ON fa1.actor_id = a1.id
    JOIN actors a2 ON fa2.actor_id = a2.id
    GROUP BY fa1.actor_id, fa2.actor_id
    HAVING movie_count > 1

5.  SELECT 
        directors.director_name, 
        COUNT(DISTINCT genres.genre_name) AS genre_count
    FROM movies
    JOIN genres ON genres.id = movies.genre_id
    JOIN directors on directors.id = movies.director_id
    GROUP BY directors.director_name
    HAVING genre_count >= 3
