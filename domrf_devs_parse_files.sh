#!/bin/bash

cd `dirname "$0"`

VOLUMES="-v /home/user/extradata:/home/user/extradata"
PROC_DATE=`date +%Y%m%d`
CACHE_DIR="cache_domrf"
DATA_DIR="_data"
FILES_DIR="domrf_files"
EMAIL="vgarshin@yandex.ru" 
CONT_NAME="parcesites_domrf_files"

ssh -N 95.216.150.30 -D 0.0.0.0:18080 & SSH_PID=$!

sudo mkdir "../$CACHE_DIR"
sudo mkdir -p "../$DATA_DIR/$FILES_DIR"

sudo docker run -i --name $CONT_NAME $VOLUMES extradata python -u domrf/domrf_devs_parse_files.py $DATA_DIR $PROC_DATE $CACHE_DIR $FILES_DIR $EMAIL
sudo docker rm -f $CONT_NAME

sudo rm -r "../$CACHE_DIR"
sudo cp -r /home/user/extradata/_data/* /home/usersftp/chroot/

kill -9 $SSH_PID