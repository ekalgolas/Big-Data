# --- Created by Ebean DDL
# To stop Ebean DDL generation, remove this comment and start using Evolutions

# --- !Ups

create table Movies (
  movie_id                  integer auto_increment not null,
  movie_name                varchar(255),
  rating                    double,
  constraint pk_Movies primary key (movie_id))
;

create table Ratings (
  user_id                   integer auto_increment not null,
  movie_id                  integer auto_increment not null,
  rating                    double,
  similarity                double)
;

create table Users (
  user_id                   integer auto_increment not null,
  user_weight               double,
  constraint pk_Users primary key (user_id))
;




# --- !Downs

SET FOREIGN_KEY_CHECKS=0;

drop table Movies;

drop table Ratings;

drop table Users;

SET FOREIGN_KEY_CHECKS=1;

