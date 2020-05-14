#!/usr/bin/env bash

echo 'Waiting for ElasticSearch...'

while ! nc -z ${ELASTIC_HOSTNAME} 9200; do

  sleep 0.1

done
echo 'ElasticSearch Initialization completed'

while ! nc -z ${KIBANA_HOSTNAME} 5601; do

  sleep 0.1

done
echo 'Kibana Initialization completed'

echo " Run gunicorn"
gunicorn --workers=4 -b 0.0.0.0:5000 wsgi:app --reload