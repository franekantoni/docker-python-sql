FROM python:latest
WORKDIR /code
ADD requirements.txt requirements.txt
ADD ratings.csv ratings.csv
ADD movies.csv movies.csv
RUN pip install -r requirements.txt
COPY app.py app.py

CMD ["python", "-u", "app.py"]
