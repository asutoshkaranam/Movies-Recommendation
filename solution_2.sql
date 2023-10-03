CREATE TABLE query1 AS
SELECT genre.name AS name, COUNT(*) AS moviecount
FROM genres genre, hasagenre e
WHERE genre.genreid = e.genreid GROUP BY genre.genreid;


CREATE TABLE query2 AS
SELECT genre.name AS name, AVG(rate.rating) AS rating
FROM genres genre, ratings rate, hasagenre e
WHERE rate.movieid = e.movieid AND genre.genreid = e.genreid 
GROUP BY genre.name;


CREATE TABLE query3 AS
SELECT movie.title, COUNT(rate.rating) AS countofratings
FROM movies movie, ratings rate
WHERE movie.movieid = rate.movieid
GROUP BY movie.title HAVING COUNT(rate.rating) >= 10;


CREATE TABLE query4 AS
SELECT movie.movieid, movie.title
FROM movies movie, hasagenre e, genres genre
WHERE movie.movieid = e.movieid AND genre.genreid = e.genreid
GROUP BY movie.movieid, genre.name HAVING genre.name = 'Comedy';


CREATE TABLE query5 AS
SELECT movie.title AS title, AVG(rate.rating) AS average
FROM movies movie, ratings rate
WHERE movie.movieid = rate.movieid
GROUP BY movie.title;


CREATE TABLE query6 AS
SELECT AVG(rate.rating) AS average
FROM movies movie, ratings rate, hasagenre e, genres genre
WHERE movie.movieid = e.movieid AND e.genreid = genre.genreid AND movie.movieid = rate.movieid
GROUP BY genre.name HAVING genre.name = 'Comedy';


CREATE TABLE query7 AS
SELECT AVG(rate.rating) AS average
FROM ratings rate
WHERE rate.movieid IN
(
    SELECT e.movieid
    FROM genres genre, hasagenre e   
    WHERE genre.name = 'Comedy' AND e.genreid = genre.genreid
INTERSECT
	SELECT e.movieid 
    FROM genres genre, hasagenre e   
    WHERE genre.name = 'Romance' AND e.genreid = genre.genreid
);
 
 
CREATE TABLE query8 AS
SELECT AVG(rate.rating) AS average 
FROM ratings rate
WHERE rate.movieid IN 
(
	SELECT e.movieid 
	FROM genres genre, hasagenre e   
	WHERE genre.name = 'Romance' AND e.genreid = genre.genreid
EXCEPT 
	SELECT e.movieid 
	FROM genres genre, hasagenre e   
	WHERE genre.name = 'Comedy' AND e.genreid = genre.genreid
 );
 
 
CREATE TABLE query9 AS
SELECT movieid as movie, rating as rate
FROM ratings
WHERE userid = :v1;