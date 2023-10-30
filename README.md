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

### Spark submit

- docker 내부에서

```
spark-submit --master spark://spark:7077 <python_file_location>

# with Kafka package
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 --master spark://spark:7077 spark_kafka.py
```

### Spark master

- 0.0.0.0:8080
- 0.0.0.0:18080 (history server)
- Running Application > Application Detail UI > 0.0.0.0:4040 으로 수정

### Spark worker scale up

```
docker compose up --scale spark-worker=2 -d
```

### Kafka create topic

```

docker exec -it {kafka-container-id} /bin/bash

cd /opt/bitnami/kafka
./bin/kafka-topics.sh --create --topic {topic_name} --bootstrap-server kafka:9092

```

### Kafka produce

```
docker exec -it {kafka-container-id} /bin/bash

cd /opt/bitnami/kafka
./bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --topic {topic_name} --producer.config /opt/bitnami/kafka/config/producer.properties
```

### Kafka consume

```
docker exec -it {kafka-container-id} /bin/bash

cd /opt/bitnami/kafka
./bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic transformed --consumer.config /opt/bitnami/kafka/config/consumer.properties
```
