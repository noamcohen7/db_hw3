Project hw3 in dbms course
Designed to have the following 5 queries executed and adjusted to it:

1.  SELECT * FROM movies 
    WHERE MATCH(title, description) AGAINST('action hero');

2. SELECT * FROM actors 
   WHERE MATCH(biography) AGAINST('Oscar winner');

3. SELECT c.country, g.genre, COUNT(*) AS genre_count
FROM Customers c
JOIN Rentals r ON c.customer_id = r.customer_id
JOIN Films f ON r.film_id = f.film_id
JOIN Genres g ON f.genre_id = g.genre_id
GROUP BY c.country, g.genre
HAVING COUNT(*) = (
    SELECT MAX(genre_count)
    FROM (
        SELECT c.country, g.genre, COUNT(*) AS genre_count
        FROM Customers c
        JOIN Rentals r ON c.customer_id = r.customer_id
        JOIN Films f ON r.film_id = f.film_id
        JOIN Genres g ON f.genre_id = g.genre_id
        GROUP BY c.country, g.genre
    ) AS genre_counts
    WHERE genre_counts.country =  c.country;

4. SELECT a1.actor_name AS actor1, a2.actor_name AS actor2, COUNT(*) AS movie_count
FROM film_actor  AS fa1
JOIN film_actor AS fa2 ON fa1.movie_id = fa2.movie_id AND fa1.actor_id < fa2.actor_id
JOIN actors AS a1 ON fa1.actor_id = a1.id
JOIN actors AS a2 ON fa2.actor_id = a2.id
GROUP BY fa1.actor_id, fa2.actor_id
HAVING movie_count > 1;


5. SELECT f.director_name, COUNT(DISTINCT m.genre) AS genre_count
FROM genre as g
JOIN films as f on g.id = f.genre_id;
GROUP BY f.director_name
HAVING genre_count >= 3;

