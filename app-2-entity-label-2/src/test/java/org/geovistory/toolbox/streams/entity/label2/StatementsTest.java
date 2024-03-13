package org.geovistory.toolbox.streams.entity.label2;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.quarkus.logging.Log;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import lib.FileRemover;
import lib.InjectRedpandaResource;
import lib.RedpandaResource;
import lib.StoreGetter;
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
import org.geovistory.toolbox.streams.entity.label2.lib.TopicsCreator;
import org.geovistory.toolbox.streams.entity.label2.names.InputTopicNames;
import org.geovistory.toolbox.streams.entity.label2.names.OutputTopicNames;
import org.geovistory.toolbox.streams.entity.label2.stores.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import ts.information.resource.Key;
import ts.information.resource.Value;

import java.io.IOException;
import java.time.Duration;
import java.util.*;

import static org.geovistory.toolbox.streams.entity.label2.lib.Fn.createEdgeKey;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Integration testing of the application with an embedded broker.
 * Remark: KafkaStreams and Redpanda is not restarted between tests.
 * With multiple test in the same test class, test can affect each other.
 */
@QuarkusTest
@QuarkusTestResource(RedpandaResource.class)
@TestProfile(StatementsTest.MyProfile.class)
public class StatementsTest {
    public static class MyProfile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of("quarkus.kafka-streams.application-id", StatementsTest.class.getName());
        }
    }

    @InjectRedpandaResource
    RedpandaResource redpandaResource;

    @ConfigProperty(name = "kafka-streams.state.dir")
    public String stateDir;

    @Inject
    KafkaStreams kafkaStreams;

    @Inject
    SStore sStore;
    @Inject
    EStore eStore;
    @Inject
    IprStore iprStore;
    @Inject
    SSubStore sSubStore;
    @Inject
    PEStore peStore;
    @Inject
    SObStore sObStore;

    @Inject
    InputTopicNames inputTopicNames;

    @Inject
    OutputTopicNames outputTopicNames;

    @Inject
    TopicsCreator topicsCreator;


    KafkaProducer<ts.projects.info_proj_rel.Key, ts.projects.info_proj_rel.Value> irpProducer;
    KafkaProducer<ts.information.statement.Key, StatementEnrichedValue> sProducer;
    KafkaProducer<Key, Value> eProducer;

    KafkaConsumer<Integer, ts.projects.info_proj_rel.Value> iprRepartConsumer;
    KafkaConsumer<Integer, StatementEnrichedValue> sRepartConsumer;
    KafkaConsumer<ProjectStatementKey, StatementValue> spConsumer;
    KafkaConsumer<String, EdgeValue> pedgeConsumer;


    @BeforeEach
    public void setUp() throws IOException, InterruptedException {
        redpandaResource.container.execInContainer("sh", "-c", "rpk cluster config set group_min_session_timeout_ms 250");
        redpandaResource.container.execInContainer("sh", "-c", "rpk group seek my-app-4 --to start");

        topicsCreator.createInputTopics();

        irpProducer = new KafkaProducer<>(producerProps());
        sProducer = new KafkaProducer<>(producerProps());
        eProducer = new KafkaProducer<>(producerProps());

        iprRepartConsumer = new KafkaConsumer<>(consumerPropsWithIntegerKey("consumer-g-13"));
        sRepartConsumer = new KafkaConsumer<>(consumerPropsWithIntegerKey("consumer-g-15"));

        spConsumer = new KafkaConsumer<>(consumerProps("consumer-g-16"));
        pedgeConsumer = new KafkaConsumer<>(consumerPropsWithStringKey("consumer-g-2"));
    }

    @AfterEach
    public void tearDown() {
        sProducer.close();
        irpProducer.close();

        iprRepartConsumer.close();
        spConsumer.close();
        pedgeConsumer.close();

        kafkaStreams.close();
        Log.info("clean up state directory");
        FileRemover.removeDir(this.stateDir);
    }


    @Test
    @Timeout(value = 300)
    public void testEdges() {

        sRepartConsumer.subscribe(Collections.singletonList(outputTopicNames.sRepartitioned()));
        iprRepartConsumer.subscribe(Collections.singletonList(outputTopicNames.iprRepartitioned()));
        pedgeConsumer.subscribe(Collections.singletonList(outputTopicNames.projectEdges()));


        // add statement
        sendSWithLiteral(50, 30, "i20", "i7");
        // ... add to project
        sendIpr(50, 40, true);
        sendIpr(50, 43, true);

        // add statement
        sendSWithLiteral(51, 30, "i20", "i7");
        // ... add to project
        sendIpr(51, 41, true);

        // add statement
        sendSWithEntity(52, 33, "i20", "i21");
        // ... add to project
        sendIpr(52, 42, true);

        // add statement
        sendSWithEntity(53, 33, "i20", "i23");
        // ... add to projects
        sendIpr(53, 40, true);
        sendIpr(53, 41, true);

        // add entity (inf resource)
        sendE(20, 30);
        // ... add to projects
        sendIpr(20, 40, true);
        sendIpr(20, 41, true);
        sendIpr(20, 42, true);
        sendIpr(20, 43, true);

        // add entity (inf resource)
        sendE(21, 30);
        // ... add to project
        // add entity (inf resource)
        sendE(22, 32);
        // ... add to project
        // add entity (inf resource)
        sendE(23, 32);
        // ... add to project
        sendIpr(23, 40, true);
        sendIpr(23, 41, true);


        var iprRepart = poll(iprRepartConsumer, 6);
        var s = poll(sRepartConsumer, 4);
        var edges = poll(pedgeConsumer, 9);

        // test partitioning
        var e50Partition = s.stream().filter((item) -> item.key() == 50).findFirst().get().partition();
        var repart50Partitions = iprRepart.stream()
                .filter(i -> i.value().getFkEntity() == 50 && i.partition() == e50Partition).toList();
        // assert that all ipr with fkEntity 50 are in same partition as e with pkEntity 50
        assertEquals(2, repart50Partitions.size());
        var e53Partition = s.stream().filter((item) -> item.key() == 53).findFirst().get().partition();
        var repart53Partitions = iprRepart.stream()
                .filter(i -> i.value().getFkEntity() == 53 && i.partition() == e53Partition).toList();
        assertEquals(2, repart53Partitions.size());


        // Test statement store ...
        var sStore = StoreGetter.getStore(this.sStore, kafkaStreams);
        // ... assert that 30 is the property of statement 50
        assertEquals(30, sStore.get(50).getPropertyId());
        // ... assert that 33 is the property of statement 53
        assertEquals(33, sStore.get(53).getPropertyId());
        // ... assert that has 4 items
        assertEquals(4, countItems(sStore.all()));

        // Test entity store ...
        var eStore = StoreGetter.getStore(this.eStore, kafkaStreams);
        // ... assert that has 4 items
        assertEquals(4, countItems(eStore.all()));

        // Test info project rel store ...
        var iprStore = StoreGetter.getStore(this.iprStore, kafkaStreams);
        // ... assert that has 12 items
        assertEquals(12, countItems(iprStore.all()));

        // Test project entity store ...
        var peStore = StoreGetter.getStore(this.peStore, kafkaStreams);
        // ... assert that has 6 items
        assertEquals(6, countItems(peStore.all()));

        // Test project statement by subject store ...
        var sSubStore = StoreGetter.getStore(this.sSubStore, kafkaStreams);
        // ... assert that has 6 items
        assertEquals(6, countItems(sSubStore.all()));

        // Test project statement by subject store ...
        // var sObStore = StoreGetter.getStore(this.sObStore, kafkaStreams);
        // ... assert that has 3 items
        // TODO uncomment next line
        // assertEquals(3, countItems(sObStore.all()));

        // Test edges
        var m = getKeyValueMap(edges.iterator());
        assertEquals(9, m.size());

        var item = m.get(createEdgeKey(40, "i20", 30, true, "i7"));

        // this item should exist
        assertNotNull(item);
        // this item should have a have target project entity
        // TODO uncomment following line
        // assertNotNull(item.getTargetProjectEntity());

        // TODO uncomment following lines
        // item = m.get(createEdgeKey(42, "i20", 33, true, "i21"));
        // // item should exist
        // assertNotNull(item);
        // // item should have a target entity
        // assertNotNull(item.getTargetNode().getEntity());
        // // item should not have target project entity because i21 is not in project 42
        // assertNull(item.getTargetProjectEntity());


    }

    private void sendIpr(int fkEntity, int fkProject, boolean inProject) {
        var k = ts.projects.info_proj_rel.Key.newBuilder()
                .setFkEntity(fkEntity)
                .setFkProject(fkProject)
                .build();
        var v = ts.projects.info_proj_rel.Value.newBuilder()
                .setFkEntity(fkEntity)
                .setFkProject(fkProject)
                .setIsInProject(inProject)
                .setSchemaName("").setTableName("").setEntityVersion(1).build();
        this.irpProducer.send(new ProducerRecord<>(inputTopicNames.proInfProjRel(), k, v));
    }

    private void sendSWithLiteral(int pkEntity, int propertyId, String subjectId, String objectId) {
        var kE = ts.information.statement.Key.newBuilder().setPkEntity(pkEntity).build();
        var vE = StatementEnrichedValue.newBuilder()
                .setSubjectId(subjectId)
                .setSubject(NodeValue.newBuilder()
                        .setEntity(Entity.newBuilder()
                                .setCommunityVisibilityToolbox(true)
                                .setCommunityVisibilityWebsite(true)
                                .setCommunityVisibilityDataApi(true)
                                .setFkClass(5).setPkEntity(0).build())
                        .setLabel("A")
                        .setClassId(5)
                        .build())
                .setPropertyId(propertyId)
                .setObjectId(objectId)
                .setObject(NodeValue.newBuilder()
                        .setAppellation(Appellation.newBuilder().setString("A").setFkClass(6).setPkEntity(0).build())
                        .setLabel("A")
                        .setClassId(6)
                        .build())
                .build();
        sProducer.send(new ProducerRecord<>(inputTopicNames.getStatementWithLiteral(), kE, vE));
    }

    private void sendSWithEntity(int pkEntity, int propertyId, String subjectId, String objectId) {
        var kE = ts.information.statement.Key.newBuilder().setPkEntity(pkEntity).build();
        var vE = StatementEnrichedValue.newBuilder()
                .setSubjectId(subjectId)
                .setSubject(NodeValue.newBuilder()
                        .setEntity(Entity.newBuilder()
                                .setCommunityVisibilityToolbox(true)
                                .setCommunityVisibilityWebsite(true)
                                .setCommunityVisibilityDataApi(true)
                                .setFkClass(5).setPkEntity(0).build())
                        .setLabel("A")
                        .setClassId(5)
                        .build())
                .setPropertyId(propertyId)
                .setObjectId(objectId)
                .setObject(NodeValue.newBuilder()
                        .setEntity(Entity.newBuilder()
                                .setCommunityVisibilityToolbox(true)
                                .setCommunityVisibilityWebsite(true)
                                .setCommunityVisibilityDataApi(true)
                                .setFkClass(5).setPkEntity(0).build())
                        .setLabel("A")
                        .setClassId(5)
                        .build())
                .build();
        sProducer.send(new ProducerRecord<>(inputTopicNames.getStatementWithLiteral(), kE, vE));
    }

    private void sendE(int pkEntity, int fkClass) {
        var kE = Key.newBuilder().setPkEntity(pkEntity).build();
        var vE = Value.newBuilder()
                .setPkEntity(pkEntity).setFkClass(fkClass)
                .setCommunityVisibility("{ \"toolbox\": true, \"dataApi\": true, \"website\": true}")
                .setSchemaName("").setTableName("").build();
        eProducer.send(new ProducerRecord<>(inputTopicNames.infResource(), kE, vE));
    }


    private Properties consumerProps(String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, redpandaResource.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "250");
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "200");
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        props.put("schema.registry.url", redpandaResource.getSchemaRegistryAddress());
        return props;
    }

    private Properties consumerPropsWithIntegerKey(String groupId) {
        var props = consumerProps(groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
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
            ConsumerRecords<K, V> records = consumer.poll(Duration.ofSeconds(1));
            records.forEach(result::add);
            fetched = result.size();
        }
        return result;
    }


    public static <E> int countItems(Iterator<E> iterator) {
        int count = 0;
        while (iterator.hasNext()) {
            iterator.next();
            count++;
        }
        return count;
    }

    public static <K, V> Map<K, V> getKeyValueMap(Iterator<ConsumerRecord<K, V>> iterator) {
        var m = new HashMap<K, V>();
        while (iterator.hasNext()) {
            var i = iterator.next();
            m.put(i.key(), i.value());
        }
        return m;
    }


}