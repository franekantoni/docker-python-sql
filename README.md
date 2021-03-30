# Analitics and DB with Docker-compose

This repository contains a Docker setup of two containers: analitics and database.
The goal is to create a system for loading and querying the movieLens [data](http://files.grouplens.org/datasets/movielens/ml-latest-small.zip "Zip file link") that will allow us to answer 6 questions: 

1. how many movies are there in the dataset?

2. what is the most common genre? 

3. what are the top 10 highest rated movies?

4. what are the top 5 userers with the highest number of ratings?

5. what is the oldest and what is the newset rating?

6. find all movies relesed in 1990


### Requirements

* [Docker](https://www.docker.com/ "Docker homepage")
* [Docker-Compose](https://docs.docker.com/compose/ "Docker-Compose docs")

### Quick start

To get the answers, start Docker Desktop, go to the root directory of the project and run

```
git clone https://github.com/franekantoni/docker-python-sql.git
cd docker-python-sql
docker-compose up --build
```

The anwsers will get printed in your console.

### Structure

* docker-compose.yml - docker-compose configuration
	
	To ensure a connection between databse and the main app the restart option is set to 'on-failure' on the 'analitics' container.
	It could be the case that the script tries to connect to PostgreSQL before it is up. In such a case the app.py restarts and tries again until the connection is established.
	This configuration makes the 'analitics' container restart every time it raises an error, whether or not it is caused by a failure to connect with the DB.
	
* database
	* Dockerfile - using the official PostgreSQL image: postgres:13.2

* analitics
	* Dockerfile - using the official Python image: python:latest
	* requirements.txt - list of packages to be installed at container start 
	* app.py - the main app


### data

Unziping the [ml-latest-small.zip](http://files.grouplens.org/datasets/movielens/ml-latest-small.zip "Zip file link") file produces:

* README.txt
* ratings.csv
* movies.csv
* links.csv
* tags.csv

ratings.csv and movies.csv files have all the information needed to answer the questions.

* ratings structure:
	userId | movieId | rating | timestamp
	--- | --- | --- | ---
	1 | 1 | 5.0 | 964980868

* movies structure:
	movieId | title | genres
	--- | --- | ---
	1 | Toy Story (1995) | Adventure\|Animation\|Children\|Comedy\|Fantasy

### database

Tables:

* movies
	* movieId
	* title
	* year

* ratings
	* userId
	* movieId
	* rating
	* timestamp

* genres
	* genreId
	* genre

* movie_genre - models many-to-many relationship of movies and genres
	* movieId
	* genreId

The table schema design is influenced by the answers the system needs to answer.
Question "What is the most common genre?" requires our database to be able to count the occurences of each genre. 
To achive that a separate table was created modeling the many-to-many relationship of generes and movies. Once the table is 
populated the task of grouping the rows by the genreId and couning the groups will give us the answer.
To answer "Find all movies relesed in 1990" we need to have a separate year column in movies table.

**One of the requirements of the task was to put all the relevant code into one file. The tables creation commands were moved into the app.py to better fit with this request.
Because of that, table creation commands will be discussed in the 'analitics' section.**

### analitics

app.py manages creating tables, loading the data from the remote server, populating the DB and provides answers to the 6 questions.
The script connects to the DB running in separate container with sqlalchemy library.
**The app.py contains all relevant code.**

#### Tables creation

For better readability each table creation command is contained within a separate function:

```python
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
```

#### Downloading

```download_data``` funciton handles the downloading, extracting and saving csv files into the /tmp folder.

```python
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
```

#### Populating DB

To save data from the movies.csv file we we need to: 
* split the second value of each row into a proper title and a year.
* split the third value of each row into a list of genres.

```process_movies``` function transforms and saves data into the database 

```python
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
```

Structure of data in ratings.csv file allows to transfer the data with ```COPY``` command.
```process_ratings``` function and saves ratings into the database.

```python
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
```

Downloading and populating is controlled by a high level function ```load_data```.
Once the data is loaded it persists in the database and will be available on consequent ```docker-compose up --build``` calls.
The ```CLEAR_AND_LOAD``` constant controlls whether the DB tables should be cleared and repopulated.
CSV processing and DB repopulating is the most resource and time consuming porcess.
It is advised to change the ```CLEAR_AND_LOAD``` to ```False``` after the initial run of the script.

#### Answers

1. how many movies are there in the dataset?

	```SQL
	SELECT COUNT(*)
	FROM movies
	```

2. what is the most common genre? 

	```SQL
	SELECT genres.genre, COUNT(movie_genre.genreId) AS value_occurrence 
	FROM movie_genre
	INNER JOIN genres 
	ON movie_genre.genreId = genres.genreId
	GROUP BY movie_genre.genreId, genres.genre
	ORDER BY value_occurrence DESC
	LIMIT    1;
	```

3. what are to top 10 highest rated movies?
	

	This is an ambivalent question to some extend. There is not metric provided, so the most obvious one is the average of all the ratings a movie got:
	```SQL
	SELECT movies.title, AVG(ratings.rating) AS avg_movie_rating
	FROM ratings
	INNER JOIN movies 
	ON ratings.movieId = movies.movieId
	GROUP BY movies.title
	ORDER BY avg_movie_rating DESC
	LIMIT    10;
	```

	But this metric favours movies with a very low number of ratings. Low sampling size makes it easy for groups to achive a very high or very low average socre.
	One way of combating this problem would be to set a minimum number or ratings a movie needs to have to be taken into consideration in this ranking:
	```SQL
	SELECT movies.title, AVG(ratings.rating) AS avg_movie_rating
	FROM ratings
	INNER JOIN movies 
	ON ratings.movieId = movies.movieId
	GROUP BY movies.title
	HAVING COUNT(movies.title) > 12
	ORDER BY avg_movie_rating DESC
	LIMIT    10;
	```

	There are more improvements for the 'highest rated' metric. One of them would be to normalize the ratings by dividing each rating by an average of all of the ratings a user gave. I would advise to specify the metric more clearly.

4. what are the top 5 users with the most ratings?

	```SQL
	SELECT userId, COUNT(userId) as num_of_ratings
	FROM ratings
	GROUP BY userId
	ORDER BY num_of_ratings DESC
	LIMIT    5;
	```
	There is no users file, so we cannot match the user ids with any personal information. This Query will return only the ids and number of ratings for the top 5 users.

5. what are the newst and the oldest ratings?

	```SQL
	SELECT *
	FROM ratings
	WHERE timestamp = (SELECT MAX(timestamp) FROM ratings)
	```

	```SQL
	SELECT *
	FROM ratings
	WHERE timestamp = (SELECT MIN(timestamp) FROM ratings)
	```

6. find all movies relesed in 1990

	```SQL
	SELECT title
	FROM movies
	WHERE year = 1990
	```
	








