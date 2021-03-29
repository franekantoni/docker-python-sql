# Analitics and DB with Docker-compose

This repository contains a Docker setup of two containers: analitics, database.
The goal is to create a system for loading and querying the movieLens data. 

### Requirements

* [Docker](https://www.docker.com/ "Docker homepage")
* [Docker-Compose](https://docs.docker.com/compose/ "Docker-Compose docs")

### Quick start

There are 6 questions we need to answer based on the [ml-latest-small.zip](http://files.grouplens.org/datasets/movielens/ml-latest-small.zip "Zip file link") data set:

1. how many movies are there in the dataset?

2. what is the most common genre? 

3. what are to top 10 highest rated movies?

4. what are the top 5 userers with the most ratings?

5. what are the newst and the oldest ratings?

6. find all movies relesed in 1990

To get the answers, start Docker Desktop, go to the root directory of the project and run

```docker-compose up --build```

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
	1 | Toy Story (1995) | Adventure|Animation|Children|Comedy|Fantasy

#### database

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

The table schema design is to high degree dictated by the answers the system needs to answer.

Question "What is the most common genre?" requires our database to be able to count the occurences of each genre. 
To achive that a separate table was created modeling the many-to-many relationship of generes and movies. Once the table is 
populated the task of grouping the rows by the genreId and couning the groups will give us the answer.

To answer "Find all movies relesed in 1990" we need to have a separate year column in movies table.



#### analitics

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


	```python
	process_ratings(file_path):
	"""
	Saves (userid, movieid, rating, timestamp) to ratings table
	"""
	```

Downloading and populating is controlled by a high level function ```load_data```.
Once the data is loaded it persists in the database and will be available on consequent ```docker-compose up --build``` calls.
The ```CLEAR_AND_LOAD``` constant controlls whether the DB tables should be cleared and repopulated.
CSV processing and DB repopulating is the most resource and time consuming porcess.
It is advised to change the ```CLEAR_AND_LOAD``` to ```False``` after the initial run of the script.

* Answers


	








