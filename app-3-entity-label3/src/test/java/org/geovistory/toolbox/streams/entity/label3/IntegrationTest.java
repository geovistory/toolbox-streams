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
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.avro.*;
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

import java.time.Duration;
import java.util.*;


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
    KafkaConsumer<String, LabelEdge> labelEdgeConsumer;

    @BeforeEach
    public void setUp() {
        topicsCreator.createInputTopics();

        edgeProducer = new KafkaProducer<>(producerProps());
        labelEdgeConsumer = new KafkaConsumer<>(consumerPropsWithStringKey("consumer-g-1"));

    }

    @AfterEach
    public void tearDown() {
        edgeProducer.close();
        labelEdgeConsumer.close();

        kafkaStreams.close();
        Log.info("clean up state directory");
        FileRemover.removeDir(this.stateDir);
    }


    @Test
    @Timeout(30)
    public void createLabelEdgesTest() {
        labelEdgeConsumer.subscribe(Collections.singletonList(outputTopicNames.labelEdgeByTarget()));

        // Publish test input
        sendEdge(1, "i1");
        // Consume label edges
        poll(labelEdgeConsumer, 1);


    }

    public void sendEdge(int pid, String sourceId) {
        var v = createEdge(pid, sourceId);
        var k = createEdgeKey(v);
        edgeProducer.send(new ProducerRecord<>(inputTopicNames.getProjectEdges(), k, v));
    }

    public static String createEdgeKey(EdgeValue e) {
        return createEdgeKey(e.getProjectId(), e.getSourceId(), e.getPropertyId(), e.getIsOutgoing(), e.getTargetId());
    }

    public static String createEdgeKey(int projectId, String sourceId, int propertyId, boolean isOutgoing, String targetId) {
        return projectId + "_" + sourceId + "_" + propertyId + "_" + (isOutgoing ? "o" : "i") + "_" + targetId;
    }

    private static EdgeValue createEdge(int pid, String sourceId) {
        return EdgeValue.newBuilder()
                .setProjectId(pid)
                .setStatementId(0)
                .setProjectCount(0)
                .setOrdNum(0f)
                .setSourceId(sourceId)
                .setSourceEntity(Entity.newBuilder()
                        .setFkClass(0)
                        .setCommunityVisibilityWebsite(false)
                        .setCommunityVisibilityDataApi(false)
                        .setCommunityVisibilityToolbox(false)
                        .build())
                .setSourceProjectEntity(EntityValue.newBuilder()
                        .setProjectId(0)
                        .setEntityId("0")
                        .setClassId(0)
                        .setCommunityVisibilityToolbox(true)
                        .setCommunityVisibilityDataApi(true)
                        .setCommunityVisibilityWebsite(true)
                        .setProjectVisibilityDataApi(true)
                        .setProjectVisibilityWebsite(true)
                        .setDeleted(false)
                        .build())
                .setPropertyId(0)
                .setIsOutgoing(false)
                .setTargetId("0")
                .setTargetNode(NodeValue.newBuilder().setLabel("").setId("0").setClassId(0)
                        .setEntity(
                                Entity.newBuilder()
                                        .setFkClass(0)
                                        .setCommunityVisibilityWebsite(true)
                                        .setCommunityVisibilityDataApi(true)
                                        .setCommunityVisibilityToolbox(true)
                                        .build())
                        .build())
                .setTargetProjectEntity(EntityValue.newBuilder()
                        .setProjectId(0)
                        .setEntityId("0")
                        .setClassId(0)
                        .setCommunityVisibilityToolbox(true)
                        .setCommunityVisibilityDataApi(true)
                        .setCommunityVisibilityWebsite(true)
                        .setProjectVisibilityDataApi(true)
                        .setProjectVisibilityWebsite(true)
                        .setDeleted(false)
                        .build())
                .setModifiedAt("0")
                .setDeleted(false)
                .build();
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
