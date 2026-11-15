package com.example;

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaShareConsumer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/** Service B pod: 5 threads = 5 job slots. A slot asks Kafka for work ONLY when it is free. */
public class ServiceB {
    static String pod;

    public static void main(String[] args) {
        pod = args.length > 0 ? args[0] : "pod-1";
        for (int slot = 1; slot <= 5; slot++) {
            int s = slot;
            new Thread(() -> worker(s), pod + "-slot-" + s).start();
        }
    }

    static void worker(int slot) {
        Properties p = config();
        p.put("client.id", pod + "-slot-" + slot);
        try (KafkaShareConsumer<String, String> kafka = new KafkaShareConsumer<>(p)) {
            kafka.subscribe(List.of("jobs"));
            while (true) {
                // Free slot waits here. A busy slot never gets here. Kafka keeps the job (backpressure).
                for (ConsumerRecord<String, String> job : kafka.poll(Duration.ofSeconds(5))) {
                    System.out.printf("%s/slot-%d START %s (attempt %d)%n",
                            pod, slot, job.value(), job.deliveryCount().orElse((short) 1));
                    Thread.sleep(15_000); // the long-running job
                    System.out.printf("%s/slot-%d DONE %s%n", pod, slot, job.value());
                    kafka.acknowledge(job, AcknowledgeType.ACCEPT); // ack only AFTER the job
                    kafka.commitSync(); // send the ack to Kafka
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    static Properties config() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "localhost:9092");
        p.put("group.id", "service-b-workers"); // every pod joins the SAME share group
        p.put("share.acknowledgement.mode", "explicit"); // we decide when a job is done
        p.put("share.acquire.mode", "record_limit"); // never take more than max.poll.records
        p.put("max.poll.records", "1"); // 1 job per slot
        p.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        p.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return p;
    }
}
