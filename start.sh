#!/bin/bash

sudo docker cp scripts/create-directories.sh namenode:/scripts/
sudo docker cp references/SQM_2GCell.csv namenode:/tmp/
sudo docker cp references/SQM_4GCell.csv namenode:/tmp/
sudo docker exec -it namenode chmod +x /scripts/create-directories.sh
sudo docker exec -it namenode bash /scripts/create-directories.sh

bash ./kafka/create-topics.sh