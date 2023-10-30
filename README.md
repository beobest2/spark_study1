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
./bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic {topic_name} --consumer.config /opt/bitnami/kafka/config/consumer.properties
```

### Kafka etc

```
./bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
./bin/kafka-topics.sh --bootstrap-server kafka:9092 --delete --topic {topic_name}
```

### Cassandra test


```
cqlsh -u cassandra -p cassandra
CREATE KEYSPACE test_db WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
use test_db;
CREATE TABLE users (login_id text PRIMARY KEY, user_name text, last_login timestamp);
INSERT INTO users (login_id, user_name, last_login) VALUES ('100', 'Kim', '2023-09-01 00:00:00');
INSERT INTO users (login_id, user_name, last_login) VALUES ('101', 'Lee', '2023-09-01 01:00:00');
INSERT INTO users (login_id, user_name, last_login) VALUES ('102', 'Park', '2023-09-01 02:00:00');
select * from users;
```

```
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,com.datastax.spark:spark-cassandra-connector_2.12:3.4.1 --master spark://spark:7077 spark_kafka_static_join.py
```
