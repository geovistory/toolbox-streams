package org.geovistory.toolbox.streams.utilities.consume;

import ts.information.place.Key;
import ts.information.place.Value;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.geovistory.toolbox.streams.lib.AppConfig;

import java.io.UnsupportedEncodingException;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * Helper class to consume avro messages from a topic
 */
public class ConsumeConfluentAvroTopic {

    public static void main(String[] args) throws UnsupportedEncodingException {

        // Assign topicName to string variable
        String topicName = "devlight.information.place";

        // create instance for properties to access producer configs
        //Properties props = KafkaSeederConfig.getConsumerConfig(topicName);
        Properties config = new Properties();

        // Configure the client with the URL of Schema Registry
        config.putIfAbsent("schema.registry.url", AppConfig.INSTANCE.getSchemaRegistryUrl());

        // Configure Kafka settings
        config.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, AppConfig.INSTANCE.getKafkaBootstrapServers());
        config.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, "Consumer-1-" + topicName);
        config.putIfAbsent(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        config.putIfAbsent(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        config.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Configure the client with the serializer, and the strategy to look up the schema in Apicurio Registry
        config.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        config.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        config.putIfAbsent(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

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


                for (ConsumerRecord<Key, Value> record : records) {
                    System.out.println(record.value().getGeoPoint().toString());
                    // print the offset,key and value for the consumer records.
                    System.out.printf("offset = %d, key = %s, value = %s\n",
                            record.offset(), record.key(), record.value());
                }
            }
        }
    }

}
