import time
import random
import csv
import re

from sqlalchemy import create_engine, insert, MetaData, Table

db_name = 'database'
db_user = 'username'
db_pass = 'secret'
db_host = 'db'
db_port = '5432'

# Connecto to the database
db_string = 'postgresql://{}:{}@{}:{}/{}'.format(db_user, db_pass, db_host, db_port, db_name)
db = create_engine(db_string)
metadata = MetaData(bind=None)

MOVIES_TABLE = Table(
    'movies', 
    metadata, 
    autoload=True, 
    autoload_with=db
)

from sqlalchemy.engine import reflection
insp = reflection.Inspector.from_engine(db)
print(insp.get_columns(MOVIES_TABLE))



def add_new_genre(genre_name):
	# Insert a new number into the 'numbers' table.
	db.execute("""
		INSERT INTO genres (genre)  
		VALUES ('{}');
		""".format(genre_name)
	)

def clear_tables(table_names):
	for table_name in table_names:
		db.execute("""
			TRUNCATE TABLE {} CASCADE
			""".format(table_name)
		)

def get_last_row():
	# Retrieve the last number inserted inside the 'numbers'

	result_set = db.execute("""
		SELECT genre
		FROM genres 
		WHERE genreId >= (SELECT max(genreId) FROM genres)
		LIMIT 1"""
	)  

	for (r) in result_set:  
		return r[0]

def load_ratings():
	connection = db.raw_connection()
	cursor = connection.cursor()
	with open('ratings.csv', 'r') as f:
		command = 'COPY ratings(userid, movieid, rating, timestamp) FROM STDIN WITH (FORMAT CSV, HEADER TRUE)'
		cursor.copy_expert(command, f)
		connection.commit()

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


def load_movies():
	genres_dict = {}
	with open('movies.csv', 'r') as csvfile:

		reader = csv.reader(csvfile)
		next(reader, None)  # skip the headers

		date_pattern = re.compile(r"\((\d+)\)")

		for row in reader:
			movie_id, title_date, genres_ = row
			movie_id = int(movie_id)
			title, year = clean_title(title_date, date_pattern)
			genres = genres_.split('|')
			
			#save row to movies
			db.execute("""
				INSERT INTO movies (movieId, title, year)  
				VALUES ({}, '{}', {});
				""".format(movie_id, title.replace("'", " "), year)
			)

			#save genre
			for genre in genres:

				if not genres_dict.get(genre):
					result = db.execute("""
						INSERT INTO genres (genre)  
						VALUES ('{}')
						RETURNING genres.genreid;
						""".format(genre)
					)
					genre_id = next(result)[0]
					genres_dict[genre] = genre_id

				db.execute("""
					INSERT INTO movie_genre (movieId, genreId)  
					VALUES ({}, {});
					""".format(movie_id, genres_dict[genre])
				)
			print('movie_id', movie_id)


if __name__ == '__main__':

	print('Application started')

	# clear_tables(['ratings', 'movies', 'genres', 'movie_genre'])

	# load_movies()
	# load_ratings()

	# 1 how many movies are there in the dataset?
	db_response = next(db.execute("""
		SELECT COUNT(*)
		FROM movies
		"""
	))
	print('how many movies are there in the dataset?', db_response)
	
	# 2 what is the most common genre? 
	db_response = db.execute("""
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
	db_response = db.execute("""
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
	db_response = db.execute("""
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

	# 4 what are the top 10 userers with the most ratings?
	db_response = db.execute("""
		SELECT userId, COUNT(userId) as num_of_ratings
	    FROM ratings
	    GROUP BY userId
	    ORDER BY num_of_ratings DESC
	    LIMIT    10;
		"""
	)
	print('4 what are the top 10 userers with the most ratings?')
	for r in db_response:
		print(r)




	







