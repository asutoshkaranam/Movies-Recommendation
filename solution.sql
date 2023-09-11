DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS movies;
DROP TABLE IF EXISTS taginfo;
DROP TABLE IF EXISTS genres;
DROP TABLE IF EXISTS ratings;
DROP TABLE IF EXISTS tags;
DROP TABLE IF EXISTS hasagenre;

CREATE TABLE users
(
    userid integer NOT NULL,
    name text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (userid)
);

CREATE TABLE movies
(
    movieid integer NOT NULL,
    title text NOT NULL,
    CONSTRAINT movies_pkey PRIMARY KEY (movieid)
);

CREATE TABLE taginfo
(
    tagid integer NOT NULL,
    content text NOT NULL,
    CONSTRAINT taginfo_pkey PRIMARY KEY (tagid)
);

CREATE TABLE genres
(
    genreid integer NOT NULL,
    name text NOT NULL,
    CONSTRAINT genres_pkey PRIMARY KEY (genreid)
);

CREATE TABLE ratings
(
    userid integer NOT NULL,
    movieid integer NOT NULL,
    rating numeric NOT NULL DEFAULT 0,
    "timestamp" bigint NOT NULL,
    CONSTRAINT ratings_pkey PRIMARY KEY (userid, movieid),
    CONSTRAINT movieid FOREIGN KEY (movieid)
        REFERENCES movies (movieid) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT userid FOREIGN KEY (userid)
        REFERENCES users (userid) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT rating CHECK (rating >= 0.0 AND rating <= 5.0)
);

CREATE TABLE tags
(
    userid integer NOT NULL,
    movieid integer NOT NULL,
    tagid integer NOT NULL,
    "timestamp" bigint NOT NULL,
    CONSTRAINT tags_pkey PRIMARY KEY (userid, movieid, tagid),
    CONSTRAINT movieid FOREIGN KEY (movieid)
        REFERENCES movies (movieid) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT tagid FOREIGN KEY (tagid)
        REFERENCES taginfo (tagid) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT userid FOREIGN KEY (userid)
        REFERENCES users (userid) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);

CREATE TABLE hasagenre
(
    movieid integer NOT NULL,
    genreid integer NOT NULL,
    CONSTRAINT hasagenre_pkey PRIMARY KEY (movieid, genreid),
    CONSTRAINT genreid FOREIGN KEY (genreid)
        REFERENCES genres (genreid) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT movieid FOREIGN KEY (movieid)
        REFERENCES movies (movieid) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);