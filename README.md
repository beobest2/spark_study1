# spark_lecture

```
$ docker run -it --rm -p 8888:8888 -v "$(pwd)":/home/jovyan/work jupyter/pyspark-notebook

```

# Docker Compose를 사용한 Spark, Kafka, Cassandra 설치

```
docker compose up -d
docker logs {pyspark-container-id}
# jupytor notebook URL 확인
```

```
docker exec -it {pyspark-spark-1-id} /bin/bash

-- applicaion의 event를 읽어주는 서버
./sbin/start-history-server.sh
```

spark submit

- docker 내부에서

```
spark-submit --master spark://spark:7077 <python_file_location>

# with Kafka package
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 --master spark://spark:7077 spark_kafka.py
```

spark master

- 0.0.0.0:8080
- 0.0.0.0:18080 (history server)

spark worker scale up

```
docker compose up --scale spark-worker=2 -d
```

kafka create topic

```

docker exec -it {kafka-container-id} /bin/bash

cd /opt/bitnami/kafka/bin
bin/kafka-topics.sh --create --topic {topic_name} --bootstrap-server kafka:9092

```

kafka produce

```
docker exec -it {kafka-container-id} /bin/bash

cd /opt/bitnami/kafka/bin
bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --topic {topic_name} --producer.config /opt/bitnami/kafka/config/producer.properties
```
