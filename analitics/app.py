import csv
import re
from io import BytesIO
from os import listdir
from sqlalchemy import create_engine
from urllib.request import urlopen
from zipfile import ZipFile

DB_NAME = "database"
DB_USER = "username"
DB_PASS = "secret"
DB_HOST = "db"
DB_PORT = "5432"

# Connect to the database
DB_STRING = "postgresql://{}:{}@{}:{}/{}".format(DB_USER, DB_PASS, DB_HOST, DB_PORT, DB_NAME)
DB = create_engine(DB_STRING)

ZIP_URL = "http://files.grouplens.org/datasets/movielens/ml-latest-small.zip"
DIR_NAME = "ml-latest-small"
CLEAR_AND_LOAD = True

def download_data(zip_url, dir_name, file_names):
	"""
	Downloads zip from remote url,
	saves csv files to tmp/
	returns True if all file_names were found in the unziped folder
	returns False if one or more of file_names were not found
	"""
	#download zip from remote url
	with urlopen(zip_url) as zipresp:
		with ZipFile(BytesIO(zipresp.read())) as zfile:
			zfile.extractall("tmp/")
	#check if all file_names in tmp
	if DIR_NAME in listdir("tmp/"):
		if all([file_name in listdir("tmp/ml-latest-small") for file_name in file_names]):
			return True
	return False

def drop_table(table):
	DB.execute("""
		DROP TABLE IF EXISTS {} CASCADE
	""").format(table)

def create_movies_table():
	DB.execute("""
		CREATE TABLE IF NOT EXISTS movies (
		    movieId INT PRIMARY KEY,
		    title TEXT,
		    year INT
		);
		"""
	)

def create_genres_table():
	DB.execute("""
		CREATE TABLE IF NOT EXISTS genres(
		  genreId SERIAL PRIMARY KEY,
		  genre TEXT NOT NULL
		);
		"""
	)

def create_movie_genre_table():
	DB.execute("""
		CREATE TABLE IF NOT EXISTS movie_genre(
		  movieId INT,
		  genreId INT,
		  FOREIGN KEY(movieId) REFERENCES movies(movieId),
		  FOREIGN KEY(genreId) REFERENCES genres(genreId),
		  PRIMARY KEY (movieId, genreId)
		);
	""")

def create_ratings_table():
	DB.execute("""
		CREATE TABLE IF NOT EXISTS ratings(
		  userId INT,
		  movieId INT,
		  rating FLOAT,
		  timestamp BIGINT,
		  FOREIGN KEY(movieId) REFERENCES movies(movieId)
		);
	""")

def clear_tables(table_names):
	"""clears the SQL tables"""
	for table_name in table_names:
		DB.execute("""
			TRUNCATE TABLE {} CASCADE
			""".format(table_name)
		)

def add_new_genre(genre_name):
	"""Adds new genre to genres table"""
	DB.execute("""
		INSERT INTO genres (genre)  
		VALUES ('{}');
		""".format(genre_name)
	)

def clean_title(title_date, pattern):
	"""
	Splits the title and production year.
	Should return one date
	will return more than one date if pattern is found in the movie title itself
	in such a case use the last date - the accual date should always be found at the end
	"""
	dates = pattern.findall(title_date)
	if dates:
		year = dates[-1]
		title = title_date.replace(f'({year})', '').rstrip()
	else:
		year = 0
		title = title_date
	return (title, int(year))

def process_movies(file_path):
	"""
	Saves:
	(movieId, title, year) to movies table,
	(genre) to genres table,
	(movieId, genreId) relation to movie_genre table
	"""
	print("loading movies into db...")
	genres_dict = {}
	with open(file_path, "r") as csvfile:

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
	print("movies loaded.")

def process_ratings(file_path):
	"""
	Saves (userid, movieid, rating, timestamp) to ratings table
	"""
	print("loading ratings into db...")
	connection = DB.raw_connection()
	cursor = connection.cursor()
	with open(file_path, "r") as f:
		command = "COPY ratings(userid, movieid, rating, timestamp) FROM STDIN WITH (FORMAT CSV, HEADER TRUE)"
		cursor.copy_expert(command, f)
		connection.commit()
	print("ratings loaded.")

def load_data():
	"""
	Downloads the zip file and populates database.
	Returns True if the data was downloaded, False if not
	"""
	movies_file = "movies.csv"
	ratings_file = "ratings.csv"
	is_data_downloaded = download_data(ZIP_URL, DIR_NAME, [movies_file, ratings_file])
	if not is_data_downloaded:
		print("data was not downloaded.")
		return False
	else:
		print("data downloaded...")
		clear_tables(["ratings", "movies", "genres", "movie_genre"])
		process_movies(f"tmp/{DIR_NAME}/{movies_file}")
		process_ratings(f"tmp/{DIR_NAME}/{ratings_file}")

if __name__ == "__main__":

	if CLEAR_AND_LOAD:
		#create tables
		create_movies_table()
		create_genres_table()
		create_movie_genre_table()
		create_ratings_table()
		#download data, populate tables
		load_data()

	# 1 how many movies are there in the dataset?
	db_response = next(DB.execute("""
		SELECT COUNT(*)
		FROM movies
		"""
	))

	print("1) how many movies are there in the dataset?")
	print("-", db_response[0], "\n")

	
	# 2 what is the most common genre? 
	genre, num = next(DB.execute("""
		SELECT genres.genre, COUNT(movie_genre.genreId) AS value_occurrence 
	    FROM movie_genre
	    INNER JOIN genres 
	    ON movie_genre.genreId = genres.genreId
	    GROUP BY movie_genre.genreId, genres.genre
	    ORDER BY value_occurrence DESC
	    LIMIT    1;
		"""
	))
	print("2) what is the most common genre?")
	print(f"- {genre}, there are {num} movies of this genre in the dataset", "\n")


	# 3 what are to top 10 highest rated movies?
	print("3) what are to top 10 highest rated movies?")
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
	print("movies with highest rating average:")
	for r in db_response:
		title, avg_rating = r
		print(f"title: {title}, rating: {avg_rating}")
	print('\n')

	#highest average with condition
	n = 10
	db_response = DB.execute("""
		SELECT movies.title, AVG(ratings.rating) AS avg_movie_rating
	    FROM ratings
	    INNER JOIN movies 
	    ON ratings.movieId = movies.movieId
	    GROUP BY movies.title
	    HAVING COUNT(movies.title) > {}
	    ORDER BY avg_movie_rating DESC
	    LIMIT    10;
		""".format(n)
	)
	print(f"movies with highest rating average, haing at least {n} ratings:")
	for r in db_response:
		title, avg_rating = r
		print(f"title: {title}, rating: {avg_rating}")
	print("\n")


	# 4 what are the top 5 userers with the most ratings?
	db_response = DB.execute("""
		SELECT userId, COUNT(userId) as num_of_ratings
	    FROM ratings
	    GROUP BY userId
	    ORDER BY num_of_ratings DESC
	    LIMIT    5;
		"""
	)
	print("4) what are the top 5 userers with the most ratings?")
	for r in db_response:
		user_id, num_ratings = r
		print(f"user id: {user_id}, number of ratings: {num_ratings}")
	print("\n")


	# 5 what are the newst and the oldest ratings?
	print("5) what are the newst and the oldest ratings?")
	#newest
	db_response = next(DB.execute("""
		SELECT *
	    FROM ratings
	    WHERE timestamp = (SELECT MAX(timestamp) FROM ratings)
		"""
	))
	print(f"newest rating: {db_response}")

	#oldest
	db_response = next(DB.execute("""
		SELECT *
	    FROM ratings
	    WHERE timestamp = (SELECT MIN(timestamp) FROM ratings)
		"""
	))
	print(f"oldest rating: {db_response}", "\n")


	# 6 find all movies relesed in 1990
	print("6) find all movies relesed in 1990:")
	db_response = DB.execute("""
		SELECT title
	    FROM movies
	    WHERE year = 1990
		"""
	)
	for movie in db_response:
		print(movie[0])

	



	







