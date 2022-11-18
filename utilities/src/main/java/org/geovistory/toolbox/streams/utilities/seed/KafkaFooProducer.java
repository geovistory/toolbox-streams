package org.geovistory.toolbox.streams.utilities.seed;

import dev.projects.project.Key;
import dev.projects.project.Value;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.Properties;

/**
 * Helper class to produce Foo messages
 */
public class KafkaFooProducer {

    public static void main(String[] args) {

        // Assign topicName to string variable
        String topicName = KafkaSeederConfig.fooTopicName;

        // create instance for properties to access producer configs
        Properties props = KafkaSeederConfig.getProducerConfig();

        Producer<Key, Value> producer = new KafkaProducer
                <>(props);

        for (int i = 0; i < 10; i++) {
            Value fooValue = Value.newBuilder().setNotes("Project 1").build();
            Key fooKey = new Key(i);
            producer.send(new ProducerRecord<>(
                    topicName,
                    fooKey,
                    fooValue)
            );
            System.out.println("Foo sent successfully");
        }
        producer.close();
    }


}
