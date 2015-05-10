CREATE DATABASE project;
USE project;

CREATE TABLE Users (
    user_id INT,
    user_weight DOUBLE
);
CREATE TABLE Ratings (
    user_id INT,
    movie_id INT,
    rating DOUBLE,
    similarity DOUBLE
);
CREATE TABLE Movies (
    movie_id INT,
    movie_name VARCHAR(50),
    rating DOUBLE
);


INSERT INTO users VALUES(1,1);
INSERT INTO users VALUES(2,1);
INSERT INTO users VALUES(3,1);

INSERT INTO movies VALUES(1,'Avengers:Age of Ultron', 9);
INSERT INTO ratings VALUES(1,1,9,1);

INSERT INTO movies VALUES(2,'Furios7', 8);
INSERT INTO ratings VALUES(2,2,8,1);

INSERT INTO movies VALUES(3,'PaulBlart:MallCop2', 7);
INSERT INTO ratings VALUES(3,3,7,1);