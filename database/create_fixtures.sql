
CREATE TABLE movies (
    movieId INT PRIMARY KEY,
    title TEXT,
    year INT
);

CREATE TABLE genres(
  genreId SERIAL PRIMARY KEY,
  genre TEXT NOT NULL
);

CREATE TABLE movie_genre(
  movieId INT,
  genreId INT,
  FOREIGN KEY(movieId) REFERENCES movies(movieId),
  FOREIGN KEY(genreId) REFERENCES genres(genreId),
  PRIMARY KEY (movieId, genreId)
);

CREATE TABLE ratings(
  userId INT,
  movieId INT,
  rating FLOAT,
  timestamp BIGINT,
  FOREIGN KEY(movieId) REFERENCES movies(movieId)
);