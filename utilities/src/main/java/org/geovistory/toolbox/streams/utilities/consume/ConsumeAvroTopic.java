package org.geovistory.toolbox.streams.utilities.consume;

import dev.projects.project.Key;
import dev.projects.project.Value;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Serdes;
import org.geovistory.toolbox.streams.lib.AppConfig;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * Helper class to consume avro messages from a topic
 */
public class ConsumeAvroTopic {

    public static void main(String[] args) {

        // Assign topicName to string variable
        String topicName = "ts_dev_0_1_0_pr_12_2-profile_with_project_properties-changelog";

        // create instance for properties to access producer configs
        //Properties props = KafkaSeederConfig.getConsumerConfig(topicName);
        Properties config = new Properties();

        // Configure Kafka settings
        config.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, AppConfig.INSTANCE.getKafkaBootstrapServers());
        config.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, "Consumer-7-" + topicName);
        config.putIfAbsent(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        config.putIfAbsent(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        config.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Configure the client with the serializer, and the strategy to look up the schema in Apicurio Registry
        config.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
//                AvroKafkaDeserializer.class.getName());
                Serdes.Integer().deserializer().getClass().getName());


        try (KafkaConsumer<Key, Value> consumer = new KafkaConsumer
                <>(config)) {

            //Kafka Consumer subscribes list of topics here.
            consumer.subscribe(List.of(topicName));

            //print the topic name
            System.out.println("Subscribed to topic " + topicName);

            //consumer.seekToBeginning(consumer.assignment());
            //consumer.poll(Duration.ofMillis(0));

            //consumer.beginningOffsets(consumer.assignment()).forEach(consumer::seek);

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
