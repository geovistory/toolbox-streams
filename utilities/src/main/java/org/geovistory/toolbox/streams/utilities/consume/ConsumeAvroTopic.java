package org.geovistory.toolbox.streams.utilities.consume;

import dev.projects.project.Key;
import dev.projects.project.Value;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.geovistory.toolbox.streams.utilities.seed.KafkaSeederConfig;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * Helper class to consume avro messages from a topic
 */
public class ConsumeAvroTopic {

    public static void main(String[] args) {

        // Assign topicName to string variable
        String topicName = "ts-dev-0.1.0-pr-10.2.project_profile";

        // create instance for properties to access producer configs
        Properties props = KafkaSeederConfig.getConsumerConfig(topicName);

        try (KafkaConsumer<Key, Value> consumer = new KafkaConsumer
                <>(props)) {

            //Kafka Consumer subscribes list of topics here.
            consumer.subscribe(List.of(topicName));

            //print the topic name
            System.out.println("Subscribed to topic " + topicName);

            //noinspection InfiniteLoopStatement
            while (true) {
                ConsumerRecords<Key, Value> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<Key, Value> record : records)

                    // print the offset,key and value for the consumer records.
                    System.out.printf("offset = %d, key = %s, value = %s\n",
                            record.offset(), record.key(), record.value());
            }
        }
    }

}
