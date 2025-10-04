source venv/bin/activate
pip install -r requirements.txt
python3 generate_parquet.py $1

new_data=$1".snappy.parquet"

sudo docker cp ${new_data} namenode:/tmp/

sudo docker exec -it namenode hdfs dfs -put /tmp/${new_data} /streaming/sales/input/

sudo docker exec -it namenode rm -rf /tmp/${new_data}
