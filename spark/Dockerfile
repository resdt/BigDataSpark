FROM jupyter/pyspark-notebook:spark-3.3.0

WORKDIR /home/jovyan/work
COPY . /home/jovyan/work

RUN pip install -r requirements.txt
RUN wget https://jdbc.postgresql.org/download/postgresql-42.6.0.jar -O /home/jovyan/postgresql-jdbc.jar
