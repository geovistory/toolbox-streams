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
import org.apache.kafka.streams.KafkaStreams;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.avro.ProjectStatementKey;
import org.geovistory.toolbox.streams.avro.StatementEnrichedValue;
import org.geovistory.toolbox.streams.avro.StatementValue;
import org.geovistory.toolbox.streams.entity.label2.lib.TopicsCreator;
import org.geovistory.toolbox.streams.entity.label2.names.InputTopicNames;
import org.geovistory.toolbox.streams.entity.label2.names.OutputTopicNames;
import org.geovistory.toolbox.streams.entity.label2.stores.SwlStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.time.Duration;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Integration testing of the application with an embedded broker.
 * Remark: KafkaStreams and Redpanda is not restarted between tests.
 * With multiple test in the same test class, test can affect each other.
 */
@QuarkusTest
@QuarkusTestResource(RedpandaResource.class)
@TestProfile(JoinIPRtoSTest.MyProfile.class)
public class JoinIPRtoSTest {
    public static class MyProfile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of("quarkus.kafka-streams.application-id", JoinIPRtoSTest.class.getName());
        }
    }

    @InjectRedpandaResource
    RedpandaResource redpandaResource;

    @ConfigProperty(name = "kafka-streams.state.dir")
    public String stateDir;

    @Inject
    KafkaStreams kafkaStreams;

    @Inject
    SwlStore swlStore;

    @Inject
    InputTopicNames inputTopicNames;

    @Inject
    OutputTopicNames outputTopicNames;

    @Inject
    TopicsCreator topicsCreator;


    KafkaProducer<ts.projects.info_proj_rel.Key, ts.projects.info_proj_rel.Value> irpProducer;
    KafkaConsumer<Integer, ts.projects.info_proj_rel.Value> iprRepartConsumer;


    KafkaProducer<ts.information.statement.Key, StatementEnrichedValue> sProducer;
    KafkaConsumer<Integer, StatementEnrichedValue> sRepartConsumer;

    KafkaConsumer<ProjectStatementKey, StatementValue> spConsumer;


    @BeforeEach
    public void setUp() throws IOException, InterruptedException {
        redpandaResource.container.execInContainer("sh", "-c", "rpk cluster config set group_min_session_timeout_ms 250");
        redpandaResource.container.execInContainer("sh", "-c", "rpk group seek my-app-4 --to start");

        topicsCreator.createInputTopics();

        irpProducer = new KafkaProducer<>(producerProps());
        sProducer = new KafkaProducer<>(producerProps());

        iprRepartConsumer = new KafkaConsumer<>(consumerPropsWithIntegerKey("consumer-g-13"));
        sRepartConsumer = new KafkaConsumer<>(consumerPropsWithIntegerKey("consumer-g-15"));

        spConsumer = new KafkaConsumer<>(consumerProps("consumer-g-16"));
    }

    @AfterEach
    public void tearDown() {
        sProducer.close();
        irpProducer.close();

        iprRepartConsumer.close();
        spConsumer.close();

        kafkaStreams.close();
        Log.info("clean up state directory");
        FileRemover.removeDir(this.stateDir);
    }


    @Test
    @Timeout(value = 300)
    public void testJoinSWL() {

        sRepartConsumer.subscribe(Collections.singletonList(outputTopicNames.sRepartitioned()));
        iprRepartConsumer.subscribe(Collections.singletonList(outputTopicNames.iprRepartitioned()));
        spConsumer.subscribe(Collections.singletonList(outputTopicNames.projectStatement()));

        // add info project rels
        sendIpr(50, 40, true);
        sendIpr(50, 41, true);
        sendIpr(50, 42, true);

        // add statements
        sendS(50, 30);
        sendS(51, 30);
        sendS(52, 32);
        sendS(53, 32);

        // add info project rels
        sendIpr(50, 43, true);
        sendIpr(53, 40, true);
        sendIpr(53, 41, true);

        var iprRepart = poll(iprRepartConsumer, 6);
        var s = poll(sRepartConsumer, 4);
        var sp = poll(spConsumer, 6);

        // test partitioning
        var e50Partition = s.stream().filter((item) -> item.key() == 50).findFirst().get().partition();
        var repart50Partitions = iprRepart.stream()
                .filter(i -> i.value().getFkEntity() == 50 && i.partition() == e50Partition).toList();
        // assert that all ipr with fkEntity 50 are in same partition as e with pkEntity 50
        assertEquals(repart50Partitions.size(), 4);

        var e53Partition = s.stream().filter((item) -> item.key() == 53).findFirst().get().partition();
        var repart53Partitions = iprRepart.stream()
                .filter(i -> i.value().getFkEntity() == 53 && i.partition() == e53Partition).toList();
        assertEquals(repart53Partitions.size(), 2);


        // test stores
        var swlStore = StoreGetter.getStore(this.swlStore, kafkaStreams);

        var val = swlStore.get(50);
        // assert that the property of entity 50 is 30
        assertEquals(30, val.getPropertyId());

        val = swlStore.get(53);
        // assert that the class of entity 53 is 32
        assertEquals(val.getPropertyId(), 32);


        assertEquals(6, sp.size());
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

    private void sendS(int pkEntity, int propertyId) {
        var kE = ts.information.statement.Key.newBuilder().setPkEntity(pkEntity).build();
        var vE = StatementEnrichedValue.newBuilder()
                .setSubjectId("i" + 1)
                .setPropertyId(propertyId)
                .setObjectId("i" + 2)
                .build();
        sProducer.send(new ProducerRecord<>(inputTopicNames.getStatementWithLiteral(), kE, vE));
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

}