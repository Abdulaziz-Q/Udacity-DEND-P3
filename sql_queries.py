import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events(
    artist varchar,
    auth varchar,
    firstName varchar,
    gender varchar,
    itemInSession integer,
    lastName varchar,
    length float, 
    level varchar,
    location varchar,
    method varchar,
    page varchar,
    registration float,
    sissionId integer,
    song varchar,
    status integer,
    ts integer,
    userAgent varchar,
    userId integer)
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs(
    num_songs integer,
    artist_id varchar,
    artist_latitude numeric,
    artist_longitude numeric,
    artist_location varchar,
    artist_name varchar,
    song_id varchar,
    title varchar,
    duration float,
    year integer)
""")

songplay_table_create = ("""
CREATE TABLE songplays(
    songplay_id integer not null primary key, 
    start_time time not null, 
    user_id integer not null, 
    level varchar not null, 
    song_id integer not null, 
    artist_id integer not null, 
    session_id integer not null, 
    location varchar not null, 
    userAgent varchar not null)
""")

user_table_create = ("""
CREATE TABLE users(
    user_id integer not null primary key, 
    first_name varchar not null, 
    last_name varchar not null, 
    gender varchar not null, 
    level varchar not null)
""")

song_table_create = ("""
CREATE TABLE song(
    song_id integer not null primary key, 
    title varchar not null, 
    artist_id integer not null, 
    year smallint not null, 
    duration varchar not null)
""")

artist_table_create = ("""
CREATE TABLE artists(
    artist_id varchar not null primary key, 
    artist_name varchar not null, 
    location varchar not null, 
    lattitude numeric, 
    longitude numeric)
""")

time_table_create = ("""
CREATE TABLE time(
    start_time time not null, 
    hour integer not null, 
    day integer not null, 
    week integer not null, 
    month integer not null, 
    year integer not null, 
    weekday integer not null)
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events
from {}
iam_role {}
json {}
""").format(config.get("S3","LOG_DATA"),config.get("IAM_ROLE","ARN"),config.get("S3","LOG_JSONPATH"))

staging_songs_copy = ("""
COPY staging_songs
from {}
iam_role {}
json as "auto"
""").format(config.get("S3","SONG_DATA"),config.get("IAM_ROLE","ARN"))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, userAgent)
SELECT TIMESTAMP 'epoch' + events.ts * interval '1 second' AS start_time,
    events.userId as user_id,
    events.level,
    songs.song_id,
    songs.artist_id,
    events.sissionId as session_id,
    events.location,
    events.userAgent
FROM staging_events events, staging_songs songs
WHERE events.page = "NextSong" 
AND events.artist = songs.artist_name 
AND events.length = songs.duration 
""")

user_table_insert = ("""
INSERT INTO users(user_id, first_name, last_name,  gender, level)
SELECT DISTINCT
    events.userId as user_id,
    events.firstName as first_name,
    events.lastName as last_name,
    events.gender,
    events.level
FROM staging_events events
WHERE page = 'NextSong'
AND userId NOT IN (SELECT DISTINCT userId FROM users)
""")

song_table_insert = ("""
INSERT INTO song(song_id, title, artist_id, year, duration)
SELECT DISTINCT
    songs.song_id,
    songs.title,
    songs.artist_id, 
    songs.year,
    songs.duration
FROM staging_songs songs
WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
INSERT INTO artists(artist_id, name, location, lattitude, longitude)
SELECT DISTINCT
    songs.artist_id,
    songs.artist_name,
    songs.artist_location as location,
    songs.artist_latitude as lattitude,
    songs.artist_longitude as longitude
FROM staging_songs songs
WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
INSERT INTO time(start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT
    events.ts,
    EXTRACT(hour FROM events.ts),
    EXTRACT(day FROM events.ts),
    EXTRACT(week FROM events.ts),
    EXTRACT(month FROM events.ts),
    EXTRACT(year FROM events.ts),
    EXTRACT(weekday FROM events.ts)
FROM  staging_events events
WHERE ts IS NOT NULL
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
