import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.quarkus.logging.Log;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.entity.label2.InputTopicNames;
import org.geovistory.toolbox.streams.lib.TsAdmin;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import ts.information.resource.Key;
import ts.information.resource.Value;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Integration testing of the application with an embedded broker.
 * Remark: KafkaStreams and Redpanda is not restarted between tests.
 * With multiple test in the same test class, test can affect each other.
 */
@QuarkusTest
@QuarkusTestResource(RedpandaResource.class)
public class TopologyProducerTest {

    @Inject
    KafkaStreams kafkaStreams;

    @InjectRedpandaResource
    RedpandaResource redpandaResource;

    @Inject
    InputTopicNames inputTopicNames;

    @ConfigProperty(name = "ts.output.topic.partitions")
    int outputTopicPartitions;
    @ConfigProperty(name = "ts.output.topic.replication.factor")
    short outputTopicReplicationFactor;

    KafkaProducer<ts.information.resource.Key, ts.information.resource.Value> infResourceProducer;

    KafkaConsumer<ts.information.resource.Key, ts.information.resource.Value> testConsumer;

    @BeforeEach
    public void setUp() {

        infResourceProducer = new KafkaProducer<>(producerProps());
        testConsumer = new KafkaConsumer<>(consumerProps());

        // create output topics (with number of partitions and delete.policy=compact)
        var admin = new TsAdmin(redpandaResource.getBootstrapServers());
        var topics = new ArrayList<String>();
        topics.add(inputTopicNames.infResource());
        admin.createOrConfigureTopics(topics, outputTopicPartitions, outputTopicReplicationFactor);
    }

    @AfterEach
    public void tearDown() {
        infResourceProducer.close();
        testConsumer.close();
        kafkaStreams.close();
        Log.info("clean up state directory");
        kafkaStreams.cleanUp();
    }

    @Test
    @Timeout(value = 300)
    public void testTopology() {

        testConsumer.subscribe(Collections.singletonList("test"));

        var projectId = 10;
        var entityId = 20;
        var classId = 30;
        var kE = ts.information.resource.Key.newBuilder().setPkEntity(entityId).build();
        var vE = ts.information.resource.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setPkEntity(entityId)
                .setFkClass(classId)
                .setCommunityVisibility("{ \"toolbox\": true, \"dataApi\": true, \"website\": true}")
                .build();
        infResourceProducer.send(new ProducerRecord<>(inputTopicNames.infResource(),
                kE,
                vE)
        );

        List<ConsumerRecord<Key, Value>> results = poll(testConsumer, 1);

        // Assumes the state store was initially empty
        assertEquals(1, results.size());
    }

    private Properties consumerProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, redpandaResource.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-id");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        props.put("schema.registry.url", redpandaResource.getSchemaRegistryAddress());
        return props;
    }

    private Properties producerProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, redpandaResource.getBootstrapServers());
        props.put("schema.registry.url", redpandaResource.getSchemaRegistryAddress());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        return props;
    }

    private List<ConsumerRecord<ts.information.resource.Key, ts.information.resource.Value>> poll(KafkaConsumer<Key, Value> consumer, int expectedRecordCount) {
        int fetched = 0;
        List<ConsumerRecord<ts.information.resource.Key, ts.information.resource.Value>> result = new ArrayList<>();
        while (fetched < expectedRecordCount) {
            ConsumerRecords<ts.information.resource.Key, ts.information.resource.Value> records = consumer.poll(Duration.ofSeconds(1));
            records.forEach(result::add);
            fetched = result.size();
        }
        return result;
    }
}