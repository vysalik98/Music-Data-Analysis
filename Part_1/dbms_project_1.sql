-- PART 1 : Tables Creation 

-- Users Table 
create table dbms.users (
user_id int ,
first_name varchar(300),
last_name varchar(300),
gender varchar  check (gender IN ('Male', 'Female')),
subscription_type varchar(300)
);
ALTER TABLE dbms.users ADD PRIMARY KEY (user_id);

-- Artists Table 
create table dbms.artists (
artist_id int,
name varchar(300) UNIQUE NOT NULL,
country varchar(300),
city varchar(300),
dob date,
PRIMARY KEY(artist_id,name)
);

-- Songs Table 
create table dbms.songs (
song_id varchar(300),
title varchar(300),
duration float,
singer varchar(300),
genre varchar(300),
release_year int
);
ALTER TABLE dbms.songs ADD PRIMARY KEY (song_id);
ALTER TABLE dbms.songs 
ADD CONSTRAINT fk_artist_name FOREIGN KEY (singer) REFERENCES dbms.artists (name);
ALTER TABLE dbms.songs 
ADD CONSTRAINT fk_genre FOREIGN KEY (genre) REFERENCES dbms.genre (genre);

-- Session Table 
create table dbms.session 
(
session_id varchar(300),
session_date date,
session_start_time time,
session_end_time time,
platform  varchar(50)
);
ALTER TABLE dbms.session ADD PRIMARY KEY (session_id);

-- Music_stream Table 
create table dbms.music_stream (
session_id varchar(300),
songs varchar[],
subscriber int
);
ALTER TABLE dbms.music_stream ADD PRIMARY KEY (session_id,subscriber);
ALTER TABLE dbms.music_stream 
ADD CONSTRAINT fk_session FOREIGN KEY (session_id) REFERENCES dbms.session (session_id);
ALTER TABLE dbms.music_stream
ADD CONSTRAINT fk_user FOREIGN KEY (subscriber) REFERENCES dbms.users (user_id);

-- Genre Table
create table dbms.genre (genre varchar(300) PRIMARY KEY);

-- Genrerel Table 
create table dbms.genrerel 
(
genre varchar(300), 
artist varchar(300),
CONSTRAINT fk_genre_name FOREIGN KEY (genre) REFERENCES dbms.genre (genre),
CONSTRAINT fk_artist_name FOREIGN KEY (artist) REFERENCES dbms.artists (name)
);

-- Part 2 : Data Preprocessing & Cleaning, Loading data to the tables
COPY dbms.users  
FROM '/tmp/users_data.csv'
DELIMITER ','
CSV HEADER;

COPY dbms.artists 
FROM '/tmp/artist_data.csv'
DELIMITER ','
CSV HEADER
encoding 'windows-1251';

COPY dbms.songs 
(song_id,title,duration,singer,genre,release_year)
FROM '/tmp/songs_data.csv'
CSV HEADER 
DELIMITER ','
encoding 'windows-1251';

COPY dbms.session
(session_id,session_date,session_start_time,session_end_time,platform)
FROM '/tmp/session_data.csv'
CSV HEADER 
DELIMITER ','
encoding 'windows-1251';

COPY dbms.music_stream
(session_id,songs,subscriber)
FROM '/tmp/music_stream.csv'
CSV HEADER 
DELIMITER E'\t'
encoding 'windows-1251';

COPY dbms.genre
(genre)
FROM '/tmp/genre.csv'
CSV HEADER 
DELIMITER ','

COPY dbms.genrerel
(genre,artist)
FROM '/tmp/genrerel.csv'
CSV HEADER 
DELIMITER ','
encoding 'windows-1251';

-- Cleaning Data

-- Finding Duplicates in genrerel 
select artist, genre, count(*)  from dbms.new_table
group by artist, genre 
having count(*) > 1;

-- Delete duplicates in Genre Rel
delete  from dbms.genrerel a 
using dbms.genrerel b where a=b and a.ctid < b.ctid;


-- Part 3 : Query-Based Analysis

-- 1. What percentage of users are free users vs Premium users?

select subscription_type 
,round((count(distinct user_id)::decimal / (select count(*) from dbms.users)::decimal) * 100, 2)  
as "percentage_users" 
from dbms.users
group by subscription_type 
order by subscription_type;

-- 2. List the name and country of artists who sang in Jazz and Pop
select name, country from dbms.artists	
where name in (
select distinct artist  from dbms.genrerel 
where lower(genre) IN('jazz','pop'))

-- 3. Of the premium users, get the details of the user (userid, firstname, lastname, gender) who is most active on the App.
select xyz.user_id as user_id, 
xyz.first_name as first_name,
xyz.last_name as last_name,
xyz.gender as gender
from (
select m.subscriber as user_id, u.first_name, u.last_name, u.gender, count(distinct s.session_id) as num_session 
,DENSE_RANK() OVER (ORDER BY count(distinct s.session_id) DESC) as rank
from dbms.music_stream m, dbms.session s, dbms.users u
where  m.session_id = s.session_id
and m.subscriber = u.user_id
and lower(u.subscription_type) = 'premium'
and lower(s.platform) = 'app'
group by m.subscriber, u.first_name, u.last_name, u.gender) xyz
where xyz.rank = 1 

-- 4. List the pair of songs & singers who sang more than 3 songs.
select singer,songs_list from 
(
select singer, array_agg(title ORDER BY title) as songs_list,count(title) as num_songs
from dbms.songs 
group by singer
having count(title) > 3
) a

-- 5. List the top subscriber/subscribers’ details who listen to Hip Hop music.
select usrs.user_id, usr.first_name, usr.last_name, usr.gender  from (
select ms.subscriber as user_id, count(distinct ms.song_id) as num_songs
,rank() over (order by count(distinct ms.song_id) desc) as rnk
from (
select distinct subscriber, unnest(songs) as song_id 
from dbms.music_stream ) ms
where  ms.song_id in (
	select distinct song_id from dbms.songs 
	where lower(genre) = 'hip hop')
group by ms.subscriber) usrs
left join dbms.users usr
on usrs.user_id = usr.user_id
where usrs.rnk = 1

-- 6. List out the top artists, the users listen to.
select a.songs, s.singer  from (
select unnest(songs) as songs, count(distinct subscriber) as num_users
,DENSE_RANK() OVER (ORDER BY count(distinct subscriber) DESC) as rank
from dbms.music_stream
group by unnest(songs)) a, dbms.songs s
where a.songs = s.song_id
and a.rank = 1

-- 7. Calculate the total time of songs in minutes released by One Direction and display result in MM:SS format
select TO_CHAR((SUM(duration) || ' second')::interval, 'MI:SS') as total_time from dbms.songs
where singer LIKE 'One Direction';

-- 8. Get the user details (id, first name, last name) who are listening to songs released between 1990 to 2009
select distinct  usr.user_id, usr.first_name, usr.last_name, sng.title, sng.release_year
from 
(select distinct song_id ,title , release_year
from dbms.songs 
where release_year between '1990' and '2009') sng
left join 
(select subscriber, unnest(songs) as song_id
from dbms.music_stream )ms
on sng.song_id = ms.song_id
left join 
(select distinct user_id, first_name, last_name
from dbms.users) usr
on ms.subscriber = usr.user_id
where ms.song_id is not null

-- 9. For the user, Bob Gray, get the user details like id, full name, gender, user type,  what songs he listen to and what  genres those songs belong to
with temp_view as (
select usr.user_id, 
usr.first_name::text || usr.last_name AS full_name, 
usr.gender, usr.subscription_type, sng_gnr.title as song_name,sng_gnr.singer,sng_gnr.genre 
from dbms.users usr 
left join (
select strm.user_id, strm.songs, sng.singer, sng.title, sng.genre from
(select ms.subscriber as user_id, ms.songs from  
(select distinct  subscriber 
,regexp_split_to_table(array_to_string(ARRAY[songs], ',', '*'), ',') songs 
from dbms.music_stream) ms) strm
left join 
dbms.songs sng
on  
strm.songs = sng.song_id) sng_gnr
on usr.user_id = sng_gnr.user_id 
where sng_gnr.user_id  is not null)
select * from temp_view 
where lower(full_name) = 'bob gray'

-- 10. what is the hour (peak hour) when most users are active at least for one minute?
select concat_ws(' - ', session_start_hr , session_end_hr ) as peak_hour_24hr_fmt from (
select extract(hour from session_start_time) as session_start_hr
, extract(hour from session_end_time) as session_end_hr
, count(distinct session_id) as sessions
, DENSE_RANK() OVER (ORDER BY count(distinct session_id) DESC) as rank
from dbms.session
group by extract(hour from session_start_time), extract(hour from session_end_time)
) x
where x.rank = 1