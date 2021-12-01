FROM python:3
# RUN apt update & apt install libpq-dev psycopg2
RUN mkdir -p /app
RUN pip install requests
COPY requirements.txt /app
RUN pip install -r /app/requirements.txt
# Entrypoint
RUN mkdir /etc/stratio
COPY app /app
RUN mkdir -p /app/data
COPY app/data /app/data
WORKDIR /app
CMD [ "python", "api_controller.py" ]