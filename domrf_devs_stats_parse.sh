#!/bin/bash

cd `dirname "$0"`

VOLUMES="-v /home/user/extradata:/home/user/extradata"
PROC_DATE=`date +%Y%m%d`
CACHE_DIR="cache_domrf_devs_stats"
DATA_DIR="_data"
EMAIL="vgarshin@yandex.ru" 
CONT_NAME="parcesites_domrf_stats"

ssh -N 95.216.150.30 -D 0.0.0.0:18080 & SSH_PID=$!

sudo mkdir "../$CACHE_DIR"

sudo docker run -i --name $CONT_NAME $VOLUMES extradata python3 domrf/domrf_devs_stats_parse.py $DATA_DIR $PROC_DATE $CACHE_DIR $EMAIL
sudo docker rm -f $CONT_NAME

sudo rm -r "../$CACHE_DIR"
sudo cp /home/user/extradata/_data/* /home/usersftp/chroot/

kill -9 $SSH_PID