# Analitics and DB with Docker-compose

This repository contains a Docker setup of two containers: analitics, database.
The goal is to create a system for loading and querying the movieLens data. 

### Requirements

⋅⋅* [Docker](https://www.docker.com/ "Docker homepage")
⋅⋅* [Docker-Compose](https://docs.docker.com/compose/ "Docker-Compose docs")

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


