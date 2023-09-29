FROM apache/airflow:2.6.2
USER root
RUN apt-get update && apt-get install -y libgeos-dev
USER airflow
COPY requirements.txt /tmp/requirements.txt
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /tmp/requirements.txt