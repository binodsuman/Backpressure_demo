# Kafka share-group backpressure

Service A publishes jobs to one topic and never waits. Each Service B pod runs 5 threads (job slots). A slot calls `poll` only when it is free, takes one record (`share.acquire.mode=record_limit`, `max.poll.records=1`), and acknowledges only after the job finishes. Extra jobs stay in Kafka until a slot is free.

Share group `service-b-workers`:

- pod-1: 5 slots
- pod-2: 5 slots

Both pods read the same single partition. A classic consumer group cannot do that.

| File | What it shows |
| --- | --- |
| `src/main/java/com/example/ServiceA.java` | `producer.send(...)` in a loop. A never waits for B. |
| `src/main/java/com/example/ServiceB.java` | 5 threads. Each polls only when free and acknowledges after the job. Same `group.id`. |
| `pom.xml` | Java 17, `kafka-clients` 4.2.0, `slf4j-simple`. |

Needs Java 17+ and a local Kafka 4.2 install. No Docker.

## One-time setup

Default `config/server.properties` is enough for one broker.

`server.properties` uses a static quorum (`controller.quorum.voters`), so format **without** `--standalone`. Data lives in `/tmp/kraft-combined-logs`; if that folder is gone (macOS reboot, `/tmp` cleanup), Kafka exits with `No readable meta.properties files found` and you must format again.

```bash
cd ~/Documents/software/kafka_2.13-4.2.0
bin/kafka-storage.sh format -t "$(bin/kafka-storage.sh random-uuid)" -c config/server.properties
```

Skip `format` only if `/tmp/kraft-combined-logs/meta.properties` is already present.

## Before the session (~2 min)

Terminal 1 — start Kafka and leave it running:

```bash
cd ~/Documents/software/kafka_2.13-4.2.0
bin/kafka-server-start.sh config/server.properties
```

Terminal 2 — enable share groups, recreate the topic, compile:

```bash
export KAFKA_HOME=~/Documents/software/kafka_2.13-4.2.0
cd ~/Documents/Binod/Cursor_Workspace/BackPressure/Backpressure

$KAFKA_HOME/bin/kafka-features.sh --bootstrap-server localhost:9092 upgrade --feature share.version=1

$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic jobs 2>/dev/null
$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic jobs --partitions 1

mvn -q compile dependency:copy-dependencies -DoutputDirectory=target/deps
```

In every new terminal:

```bash
export KAFKA_HOME=~/Documents/software/kafka_2.13-4.2.0
cd ~/Documents/Binod/Cursor_Workspace/BackPressure/Backpressure
```

## Live demo

Start Service B first. A new share group only reads jobs published after it starts (`share.auto.offset.reset` defaults to `latest`).

Terminal 2:

```bash
java -cp "target/classes:target/deps/*" com.example.ServiceB pod-1
```

Terminal 3:

```bash
java -cp "target/classes:target/deps/*" com.example.ServiceB pod-2
```

### 1. Backpressure — publish more jobs than the 10 slots

Terminal 4:

```bash
java -cp "target/classes:target/deps/*" com.example.ServiceA 15
```

- 10 jobs START at once (5 per pod). Jobs 11–15 wait in Kafka.
- After ~15s, each DONE is followed by a START of a waiting job.
- Both pods work from one partition.

### 2. Crash = Kafka watchdog

```bash
java -cp "target/classes:target/deps/*" com.example.ServiceA 10
```

Within 5s of the START lines, press Ctrl+C in pod-1 (or `kill -9` that process). When the acquisition lock expires (default 30s), pod-1's in-flight jobs restart on pod-2 as attempt 2. The 15s job must stay shorter than that lock.

## Reset between rehearsals

Stop both pods, then re-run the "fresh topic" commands above (`--delete` and `--create`).

## Tested

Verified against the Kafka 4.2.0 client API: `KafkaShareConsumer`, explicit acknowledgement, and `record_limit` acquire mode. Single broker, 1 partition, 2 pods: 10 jobs run at once, extras wait and are picked up as slots free; after pod-1 is killed, its in-flight jobs are redelivered to pod-2 as attempt 2.
