package org.geovistory.toolbox.streams.utilities.seed;

import dev.projects.project.Key;
import dev.projects.project.Value;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.geovistory.toolbox.streams.avro.Operation;
import org.geovistory.toolbox.streams.avro.ProjectRdfKey;
import org.geovistory.toolbox.streams.avro.ProjectRdfValue;
import org.geovistory.toolbox.streams.lib.AppConfig;

import java.util.Properties;

/**
 * Helper class to produce Foo messages
 */
public class KafkaAvroProducer {

    public static void main(String[] args) {


        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put("schema.registry.url", "https://schema-registry.geovistory.org");
        KafkaProducer producer = new KafkaProducer(props);

        ProjectRdfKey key = ProjectRdfKey.newBuilder().setProjectId(1).setTurtle("<a> <b> <c>").build();
        ProjectRdfValue value = ProjectRdfValue.newBuilder().setOperation(Operation.insert).build();

        ProducerRecord<ProjectRdfKey, ProjectRdfValue> record = new ProducerRecord<>("dev-rdf-test-topic", key, value);
        try {
            producer.send(record);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            // may need to do something with it
        }
        // When you're finished producing records, you can flush the producer to ensure it has all been written to Kafka and
        // then close the producer to free its resources.
        finally {
            producer.flush();
            producer.close();
        }
    }


}
