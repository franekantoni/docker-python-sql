import time
import random
import csv
import re

from sqlalchemy import create_engine

DB_NAME = 'database'
DB_USER = 'username'
DB_PASS = 'secret'
DB_HOST = 'db'
DB_PORT = '5432'

# Connect to the database
DB_STRING = 'postgresql://{}:{}@{}:{}/{}'.format(DB_USER, DB_PASS, DB_HOST, DB_PORT, DB_NAME)
DB = create_engine(DB_STRING)

ZIP_URL = 'http://files.grouplens.org/datasets/movielens/ml-latest-small.zip'
DIR_NAME = 'ml-latest-small'


def add_new_genre(genre_name):
	# Insert a new number into the 'numbers' table.
	DB.execute("""
		INSERT INTO genres (genre)  
		VALUES ('{}');
		""".format(genre_name)
	)

def clear_tables(table_names):
	for table_name in table_names:
		DB.execute("""
			TRUNCATE TABLE {} CASCADE
			""".format(table_name)
		)

def get_last_row():
	# Retrieve the last number inserted inside the 'numbers'

	result_set = DB.execute("""
		SELECT genre
		FROM genres 
		WHERE genreId >= (SELECT max(genreId) FROM genres)
		LIMIT 1"""
	)  

	for (r) in result_set:  
		return r[0]

def load_ratings(file_path):
	print('loading ratings into db...')
	connection = DB.raw_connection()
	cursor = connection.cursor()
	with open(file_path, 'r') as f:
		command = 'COPY ratings(userid, movieid, rating, timestamp) FROM STDIN WITH (FORMAT CSV, HEADER TRUE)'
		cursor.copy_expert(command, f)
		connection.commit()
	print('ratings loaded.')

def clean_title(title_date, pattern):
	#should return one date
	#will return more than one date if pattern is found in the movie title itself
	#in such a case use the last date - the accual date should always be found at the end
	dates = pattern.findall(title_date)
	if dates:
		year = dates[-1]
		title = title_date.replace(f'({year})', '').rstrip()
	else:
		year = 0
		title = title_date
	return (title, int(year))


def load_movies(file_path):
	print('loading movies into db...')
	genres_dict = {}
	with open(file_path, 'r') as csvfile:

		reader = csv.reader(csvfile)
		next(reader, None)  # skip the headers

		date_pattern = re.compile(r"\((\d+)\)")

		for row in reader:
			movie_id, title_date, genres_ = row
			movie_id = int(movie_id)
			title, year = clean_title(title_date, date_pattern)
			genres = genres_.split('|')
			
			#save row to movies
			DB.execute("""
				INSERT INTO movies (movieId, title, year)  
				VALUES ({}, '{}', {});
				""".format(movie_id, title.replace("'", " "), year)
			)

			#save genre
			for genre in genres:

				if not genres_dict.get(genre):
					result = DB.execute("""
						INSERT INTO genres (genre)  
						VALUES ('{}')
						RETURNING genres.genreid;
						""".format(genre)
					)
					genre_id = next(result)[0]
					genres_dict[genre] = genre_id

				DB.execute("""
					INSERT INTO movie_genre (movieId, genreId)  
					VALUES ({}, {});
					""".format(movie_id, genres_dict[genre])
				)
	print('movies loaded.')

from io import BytesIO
from urllib.request import urlopen
from zipfile import ZipFile
from os import listdir

def download_data(zip_url, dir_name, file_names):
	with urlopen(zip_url) as zipresp:
		with ZipFile(BytesIO(zipresp.read())) as zfile:
			zfile.extractall('tmp/')
	if DIR_NAME in listdir('tmp/'):
		if all([file_name in listdir('tmp/ml-latest-small') for file_name in file_names]):
			return True
	return False


if __name__ == '__main__':

	movies_file = 'movies.csv'
	ratings_file = 'ratings.csv'

	is_data_downloaded = download_data(ZIP_URL, DIR_NAME, [movies_file, ratings_file])

	if not is_data_downloaded:
		print('data was not downloaded.')
	else:
		print('data downloaded...')

		clear_tables(['ratings', 'movies', 'genres', 'movie_genre'])

		load_movies(f"tmp/{DIR_NAME}/{movies_file}")
		load_ratings(f"tmp/{DIR_NAME}/{ratings_file}")

		# 1 how many movies are there in the dataset?
		db_response = next(DB.execute("""
			SELECT COUNT(*)
			FROM movies
			"""
		))
		print('how many movies are there in the dataset?', db_response)
		
		# 2 what is the most common genre? 
		db_response = DB.execute("""
			SELECT genres.genre, COUNT(movie_genre.genreId) AS value_occurrence 
		    FROM movie_genre
		    INNER JOIN genres 
		    ON movie_genre.genreId = genres.genreId
		    GROUP BY movie_genre.genreId, genres.genre
		    ORDER BY value_occurrence DESC
		    LIMIT    10;
			"""
		)
		for r in db_response:
			genre, num = r
			print(f"there are {num} of {genre} movies")

		# 3 what are to top 10 highest rated movies?

		#highest average
		#tends to favour movies with a low number of ratings
		db_response = DB.execute("""
			SELECT movies.title, AVG(ratings.rating) AS avg_movie_rating
		    FROM ratings
		    INNER JOIN movies 
		    ON ratings.movieId = movies.movieId
		    GROUP BY movies.title
		    ORDER BY avg_movie_rating DESC
		    LIMIT    10;
			"""
		)
		for r in db_response:
			print(r)

		#highest average with condition
		db_response = DB.execute("""
			SELECT movies.title, AVG(ratings.rating) AS avg_movie_rating
		    FROM ratings
		    INNER JOIN movies 
		    ON ratings.movieId = movies.movieId
		    GROUP BY movies.title
		    HAVING COUNT(movies.title) > 10
		    ORDER BY avg_movie_rating DESC
		    LIMIT    10;
			"""
		)
		for r in db_response:
			print(r)

		# 4 what are the top 5 userers with the most ratings?
		db_response = DB.execute("""
			SELECT userId, COUNT(userId) as num_of_ratings
		    FROM ratings
		    GROUP BY userId
		    ORDER BY num_of_ratings DESC
		    LIMIT    5;
			"""
		)
		print('4 what are the top 5 userers with the most ratings?')
		for r in db_response:
			print(r)

		# 5 what are the newst and the oldest ratings?

		#newest
		db_response = next(DB.execute("""
			SELECT *
		    FROM ratings
		    WHERE timestamp = (SELECT MAX(timestamp) FROM ratings)
			"""
		))
		print(f'newest rating: {db_response}')

		#oldest
		db_response = next(DB.execute("""
			SELECT *
		    FROM ratings
		    WHERE timestamp = (SELECT MIN(timestamp) FROM ratings)
			"""
		))
		print(f'oldest rating: {db_response}')

		# 6 find all movies relesed in 1990
		db_response = DB.execute("""
			SELECT *
		    FROM movies
		    WHERE year = 1990
			"""
		)
		for movie in db_response:
			print(movie)

	



	







