# QUERIES TO RUN FOR SPARK DATA LAKE PROJECT

# Basic analytic queries
songplays_full_data = ("""
    SELECT song_id, artist_id 
    FROM songplays
    WHERE song_id IS NOT NULL
    AND artist_id IS NOT NULL
""")

popular_artists = ("""
    SELECT a.name AS artist, COUNT(*) AS total_plays
    FROM songplays s
    JOIN artists a
    ON s.artist_id = a.artist_id
    GROUP BY a.name
    ORDER BY total_plays DESC
    LIMIT 10
""")

listening_locations = ("""
    SELECT location, COUNT(*) AS total_plays
    FROM songplays
    GROUP BY location
    ORDER BY total_plays DESC
    LIMIT 10
""")

# Data skewness queries on the tables we partitioned to see if our method makes sense
# Songs table was partitioned by year and artist
songs_skew = ("""
    SELECT year, artist_id, COUNT(*) AS count
    FROM songs
    GROUP BY 1, 2
    ORDER BY 3 DESC
""")

# Time table was partitioned by year and month
time_skew = ("""
    SELECT year, month, COUNT(*) AS count
    FROM time
    GROUP BY 1, 2
    ORDER BY 3 DESC
""")
