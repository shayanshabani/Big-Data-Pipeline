sudo docker cp scripts/create-directories.sh namenode:/scripts/
sudo docker exec -it namenode chmod +x /scripts/create-directories.sh
sudo docker exec -it namenode bash /scripts/create-directories.sh
