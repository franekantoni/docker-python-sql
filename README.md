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

The awsers will get printed in your console.

### Structure

* docker-compose.yml - docker-compose configuration
	
* database
	* Dockerfile - using the official PostgreSQL image: postgres:13.2
	* create_fixtures.sql - sql script to be executed on container start

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

why PostgreSQL?

* fully open-source - free
* well supported by the community

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

### analitics

app.py manages loading the data from the remote server, populating the DB and provides answers to the 6 questions.
The script connects to the DB running in separate container with sqlalchemy library.

* Downloading

	```python
	download_data(zip_url, dir_name, file_names):
	"""
	Downloads zip from remote url,
	saves csv files to tmp/
	returns True if all file_names were found in the unziped folder
	returns False if one or more of file_names were not found
	"""
	```


* Populating DB

	```python
	process_movies(file_path):
	"""
	Saves:
	(movieId, title, year) to movies table,
	(genre) to genres table,
	(movieId, genreId) relation to movie_genre table
	"""
	```
To save the data to the database we we need to: 
* split the second value of each row into a proper title and a year.
* split the third value of each row into a list of genres.

	```python
	process_ratings(file_path):
	"""
	Saves (userid, movieid, rating, timestamp) to ratings table
	"""
	```
Structure of data in ratings.csv file allows to transfer the data with ```COPY``` command


Downloading and populating is controlled by a high level function ```load_data```.
Once the data is loaded it persists in the database and will be available on consequent ```docker-compose up --build``` calls.
The ```CLEAR_AND_LOAD``` constant controlls whether the DB tables should be cleared and repopulated.
CSV processing and DB repopulating is the most resource and time consuming porcess.
It is advised to change the ```CLEAR_AND_LOAD``` to ```False``` after the initial run of the script.

* Answers

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

	There are more imporvemtns for the 'highest rated' metric. One of them would be to normalize the ratings by dividing each rating by an average of all of the ratings a user gave. I would advise to specify the metric more clearly.

4. what are the top 5 userers with the most ratings?

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
	








