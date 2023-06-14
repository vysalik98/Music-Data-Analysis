-- PART 1: Query-Based Analysis

-- 1. List out the number of sessions the subscribers are present in based on their subscription_type.
select u.subscription_type, count(session_id) as user_count
from dbms.users u 
JOIN dbms.music_stream ms 
ON u.user_id = ms.subscriber
GROUP BY u.subscription_type;

-- 2. In which year or years were the most songs recorded and how many songs are those ?
select count(title),release_year from dbms.songs 
group by release_year
having count(*) IN (select max(count) 
					from 
					    (
						select release_year, count(*) as count 
						from dbms.songs 
						group by release_year
						) a
					)

-- 3. For songs released in every year, List the number of times the songs were listened to.
select b.release_year , sum(a.num_lisened_to ) as times_streamed
from (
select  unnest(songs) as song_id, count(*) as num_lisened_to
from dbms.music_stream
group by unnest(songs)) a
left join (
select release_year, song_id  
from dbms.songs ) b
on a.song_id = b.song_id 
group by b.release_year
order by  b.release_year

-- 4. Who is the singer that was born in 1991 and sang the most of POP
select name as singer,country
from dbms.artists
where dob::text  like '1991%'
and name in 
(select distinct artist from  dbms.genrerel where lower(genre) = 'pop') 

-- 5. List out the song id premium users are listening to that is currently popular.
select song_id from (
select unnest(m.songs) as song_id, count(*) as popularity
,dense_rank() over (order by count(*) desc) as rnk
from dbms.users u, dbms.music_stream m
where u.subscription_type = 'Premium'
and  u.user_id = m.subscriber
group by unnest(m.songs)) x
where rnk = 1

-- 6. Name the populor singer(s) for ever year. Popularity will be based on the song which is most listened to. 
with tmp_tbl as (
	select s.singer, s.release_year, m.song_id, m.song_popularity
	from (
	select singer, song_id, release_year
	from dbms.songs ) s
	left join 
	(select unnest(songs) as song_id, count(subscriber) as song_popularity
	from dbms.music_stream 
	group by unnest(songs)) m
	on s.song_id = m.song_id
	where m.song_id is not null
)
select  pop.release_year, sng.singer 
	from (
	select release_year, song_id, sum(song_popularity) as tot_popularity
	,dense_rank() over (partition by release_year order by sum(song_popularity) desc) as rnk
	from tmp_tbl
	group by release_year, song_id) pop 
	left join (select distinct song_id, singer from dbms.songs) sng
	on pop.song_id = sng.song_id
where pop.rnk = 1
order by  pop.release_year

-- 7. Which artist has no genre?
explain  select distinct a.name as artist_name
from dbms.artists a
left join  
(select distinct artist from dbms.genrerel) g 
on a.name = g.artist
where g.artist is null

-- 8. List the 1998-born singers' names, the number of songs they performed, and their overall songs' lengthexplain select s.singer , count(*) as num_songs, concat(round(SUM(s.duration):: decimal,2), ' sec') as duration,a.dob
from dbms.songs s,dbms.artists a 
where dob::text like '1998%'
and a.name = s.singer
group by  s.singer,a.dob;

-- 9. How many users of the app and website are free and premium users?
with temp_data as (
	                select ses.platform, m.subscriber , u.subscription_type
	                from dbms.session ses, dbms.music_stream m, dbms.users u 
	                where ses.session_id = m.session_id
	                and m.subscriber = u.user_id
	                ) 
	                select subscription_type
					,sum(case when platform = 'App' then 1 else 0 end )as app_users
					,sum(case when platform = 'Web' then 1 else 0 end )as web_users
					from temp_data
					group by subscription_type

-- 10. Determine the longest session and identify the user details (name, id, dender) of that session 
 
explain select s.session_id,(s.session_end_time - s.session_start_time) AS session_duration,
u.first_name || ' ' || u.last_name AS full_name, u.gender, u.subscription_type
from dbms.session s
join dbms.music_stream ms on s.session_id = ms.session_id
join dbms.users u on ms.subscriber = u.user_id
order by session_duration desc
fetch first 1 row only