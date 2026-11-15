package com.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/** Service A: fire jobs into ONE topic as fast as it wants. It never waits for Service B. */
public class ServiceA {

    public static void main(String[] args) {
        int jobs = args.length > 0 ? Integer.parseInt(args[0]) : 30;

        Properties p = new Properties();
        p.put("bootstrap.servers", "localhost:9092");
        p.put("acks", "all");
        p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        p.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(p)) {
            for (int i = 1; i <= jobs; i++) {
                producer.send(new ProducerRecord<>("jobs", "job-" + i));
                System.out.println("Published job-" + i);
            }
        }
    }
}
