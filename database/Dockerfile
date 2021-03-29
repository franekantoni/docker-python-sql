FROM postgres:13.2
ENV POSTGRES_PASSWORD=secret
ENV POSTGRES_USER=username
ENV POSTGRES_DB=database
COPY create_fixtures.sql /docker-entrypoint-initdb.d/create_fixtures.sql
