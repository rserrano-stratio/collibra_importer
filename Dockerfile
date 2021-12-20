FROM python:3
# RUN apt update & apt install libpq-dev psycopg2
RUN apt-get update && apt-get install -y vim curl jq
# Stratio Utils versions
ENV B_LOG_VERSION 0.4.9
ENV KM_UTILS_VERSION 0.4.9
ENV STRATIO_UTILS /stratio

# Download Stratio Utils
RUN mkdir /stratio
ADD http://niquel.stratio.com/repository/paas/kms_utils/${KM_UTILS_VERSION}/kms_utils-${KM_UTILS_VERSION}.sh ${STRATIO_UTILS}/kms_utils.sh
ADD http://niquel.stratio.com/repository/paas/log_utils/${B_LOG_VERSION}/b-log-${B_LOG_VERSION}.sh ${STRATIO_UTILS}/b-log.sh


RUN mkdir -p /app
RUN pip install requests
COPY requirements.txt /app
RUN pip install -r /app/requirements.txt
# Entrypoint
RUN mkdir /etc/stratio
COPY app /app
RUN mkdir -p /app/data/uploads
COPY app/data /app/data
WORKDIR /app
#CMD [ "python", "api_controller.py" ]
COPY entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

EXPOSE 8000

ENTRYPOINT [ "/docker-entrypoint.sh" ]
