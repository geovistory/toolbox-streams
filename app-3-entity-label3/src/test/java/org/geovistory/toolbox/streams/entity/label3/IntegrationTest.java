package org.geovistory.toolbox.streams.entity.label3;

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
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.avro.EdgeValue;
import org.geovistory.toolbox.streams.avro.EntityLabel;
import org.geovistory.toolbox.streams.avro.EntityLabelConfigPartField;
import org.geovistory.toolbox.streams.avro.ProjectEntityKey;
import org.geovistory.toolbox.streams.entity.label3.lib.TopicsCreator;
import org.geovistory.toolbox.streams.entity.label3.names.InputTopicNames;
import org.geovistory.toolbox.streams.entity.label3.names.OutputTopicNames;
import org.geovistory.toolbox.streams.testlib.FileRemover;
import org.geovistory.toolbox.streams.testlib.InjectRedpandaResource;
import org.geovistory.toolbox.streams.testlib.RedpandaResource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import ts.projects.entity_label_config.Key;
import ts.projects.entity_label_config.Value;

import java.time.Duration;
import java.util.*;

import static org.geovistory.toolbox.streams.entity.label3.testlib.MockConfig.createLabelConfig;
import static org.geovistory.toolbox.streams.entity.label3.testlib.MockEdges.*;
import static org.junit.jupiter.api.Assertions.assertEquals;


@QuarkusTest
@QuarkusTestResource(restrictToAnnotatedClass = true, value = RedpandaResource.class)
public class IntegrationTest {

    @ConfigProperty(name = "kafka-streams.state.dir")
    public String stateDir;

    @Inject
    KafkaStreams kafkaStreams;

    @Inject
    InputTopicNames inputTopicNames;

    @Inject
    OutputTopicNames outputTopicNames;

    @Inject
    TopicsCreator topicsCreator;
    @InjectRedpandaResource
    RedpandaResource redpandaResource;

    KafkaProducer<String, EdgeValue> edgeProducer;
    KafkaProducer<Key, Value> configProducer;
    KafkaConsumer<ProjectEntityKey, EntityLabel> entityLabelConsumer;

    @BeforeEach
    public void setUp() {
        topicsCreator.createInputTopics();

        edgeProducer = new KafkaProducer<>(producerProps());
        configProducer = new KafkaProducer<>(producerProps());
        entityLabelConsumer = new KafkaConsumer<>(consumerProps("consumer-g-1"));

    }

    @AfterEach
    public void tearDown() {
        edgeProducer.close();
        configProducer.close();
        entityLabelConsumer.close();

        kafkaStreams.close();
        Log.info("clean up state directory");
        FileRemover.removeDir(this.stateDir);
    }


    @Test
    @Timeout(300)
    public void createLabelEdgesTest() {
        entityLabelConsumer.subscribe(Collections.singletonList(outputTopicNames.entityLabels()));

        // test the propagation of the label from literal to entity(c365) to entity(c21)

        // edges of appellation in a language
        sendEdge(createEdgeWithoutProjectEntity(
                1, 10, 365, 0.7f, 1113, true, nodeValueWithLabel(20, 40, "Abc"), "2021", false
        ));

        sendEdge(createEdgeWithoutProjectEntity(
                1, 10, 365, 0.7f, 1112, true, nodeValueWithLanguage(18605), "2021", false
        ));

        // edges of person
        sendEdge(createEdgeWithProjectEntity(
                1, 11, 21, 0.7f, 1111, false, "i10", 365, 1, false, "2021", false
        ));

        // label config for appellation in language
        sendLabelConfig(createLabelConfig(80, 1, 365, "false", new EntityLabelConfigPartField[]{
                new EntityLabelConfigPartField(1113, true, 1)
        }));

        // label config for person
        sendLabelConfig(createLabelConfig(81, 1, 21, "false", new EntityLabelConfigPartField[]{
                new EntityLabelConfigPartField(1111, false, 1)
        }));


        // Consume entity labels
        var el = poll(entityLabelConsumer, 2);
        var elMap = getKeyRecordMap(el.iterator());
        assertEquals("Abc", elMap.get(new ProjectEntityKey(1, "i10")).value().getLabel());
        var i11 = elMap.get(new ProjectEntityKey(1, "i11")).value();
        assertEquals("Abc", i11.getLabel());
        assertEquals("de", i11.getLanguage());

    }

    public void sendEdge(EdgeValue v) {
        var k = createEdgeKey(v);
        edgeProducer.send(new ProducerRecord<>(inputTopicNames.getProjectEdges(), k, v));
    }


    public void sendLabelConfig(KeyValue<Key, Value> kv) {
        configProducer.send(new ProducerRecord<>(inputTopicNames.proEntityLabelConfig(), kv.key, kv.value));
    }


    private Properties consumerProps(String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, redpandaResource.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        props.put("schema.registry.url", redpandaResource.getSchemaRegistryAddress());
        return props;
    }

    private Properties consumerPropsWithIntegerKey(String groupId) {
        var props = consumerProps(groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        return props;
    }

    private Properties consumerPropsWithIntegerVal(String groupId) {
        var props = consumerProps(groupId);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        return props;
    }

    private Properties consumerPropsWithStringKey(String groupId) {
        var props = consumerProps(groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
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

    static private <K, V> List<ConsumerRecord<K, V>> poll(KafkaConsumer<K, V> consumer, int expectedRecordCount) {
        int fetched = 0;
        List<ConsumerRecord<K, V>> result = new ArrayList<>();
        while (fetched < expectedRecordCount) {
            ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(100));
            records.forEach(result::add);
            fetched = result.size();
        }
        return result;
    }


    public static <K, V> Map<K, V> getKeyValueMap(Iterator<ConsumerRecord<K, V>> iterator) {
        var m = new HashMap<K, V>();
        while (iterator.hasNext()) {
            var i = iterator.next();
            m.put(i.key(), i.value());
        }
        return m;
    }


    public static <K, V> Map<K, ConsumerRecord<K, V>> getKeyRecordMap(Iterator<ConsumerRecord<K, V>> iterator) {
        var m = new HashMap<K, ConsumerRecord<K, V>>();
        while (iterator.hasNext()) {
            var i = iterator.next();
            m.put(i.key(), i);
        }
        return m;
    }

}
