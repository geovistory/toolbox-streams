package org.geovistory.toolbox.streams.project.items;

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
import org.geovistory.toolbox.streams.avro.EntityValue;
import org.geovistory.toolbox.streams.avro.ProjectEntityKey;
import org.geovistory.toolbox.streams.project.items.lib.TopicsCreator;
import org.geovistory.toolbox.streams.project.items.names.InputTopicNames;
import org.geovistory.toolbox.streams.project.items.names.OutputTopicNames;
import org.geovistory.toolbox.streams.project.items.stores.EStore;
import org.junit.jupiter.api.*;
import ts.information.resource.Key;
import ts.information.resource.Value;

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
@TestProfile(JoinIPRtoETest.MyProfile.class)
public class JoinIPRtoETest {
    public static class MyProfile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of("quarkus.kafka-streams.application-id", JoinIPRtoETest.class.getName());
        }
    }

    @InjectRedpandaResource
    RedpandaResource redpandaResource;

    @Inject
    KafkaStreams kafkaStreams;

    @ConfigProperty(name = "kafka-streams.state.dir")
    public String stateDir;

    @Inject
    EStore eStore;

    @Inject
    InputTopicNames inputTopicNames;

    @Inject
    OutputTopicNames outputTopicNames;

    @Inject
    TopicsCreator topicsCreator;


    KafkaProducer<ts.projects.info_proj_rel.Key, ts.projects.info_proj_rel.Value> irpProducer;
    KafkaConsumer<Integer, ts.projects.info_proj_rel.Value> iprRepartConsumer;

    KafkaProducer<Key, Value> eProducer;
    KafkaConsumer<Integer, Value> eRepartConsumer;


    KafkaConsumer<ProjectEntityKey, EntityValue> epConsumer;


    @BeforeEach
    public void setUp() {
        topicsCreator.createInputTopics();

        irpProducer = new KafkaProducer<>(producerProps());
        eProducer = new KafkaProducer<>(producerProps());

        iprRepartConsumer = new KafkaConsumer<>(consumerPropsWithIntegerKey("consumer-g-3"));
        eRepartConsumer = new KafkaConsumer<>(consumerPropsWithIntegerKey("consumer-g-7"));

        epConsumer = new KafkaConsumer<>(consumerProps("consumer-g-4"));
    }

    @AfterEach
    public void tearDown() {
        eProducer.close();
        irpProducer.close();

        iprRepartConsumer.close();
        eRepartConsumer.close();
        epConsumer.close();

        kafkaStreams.close();

        Log.info("clean up state directory");
        FileRemover.removeDir(this.stateDir);
    }


    @Test
    @Timeout(value = 30)
    public void testJoinE() {

        eRepartConsumer.subscribe(Collections.singletonList(outputTopicNames.eRepartitioned()));
        iprRepartConsumer.subscribe(Collections.singletonList(outputTopicNames.iprRepartitioned()));
        epConsumer.subscribe(Collections.singletonList(outputTopicNames.projectEntity()));

        // add info project rels
        sendIpr(20, 40, true);
        sendIpr(20, 41, true);
        sendIpr(20, 42, true);

        // add entities (inf resources)
        sendE(20, 30);
        sendE(21, 30);
        sendE(22, 32);
        sendE(23, 32);

        // add info project rels
        sendIpr(20, 43, true);
        sendIpr(23, 40, true);
        sendIpr(23, 41, true);
        var e = poll(eRepartConsumer, 4);
        var iprRepart = poll(iprRepartConsumer, 6);
        var ep = poll(epConsumer, 6);

        // test partitioning
        var e20Partition = e.stream().filter((item) -> item.value().getPkEntity() == 20).findFirst().get().partition();
        var repart20Partitions = iprRepart.stream()
                .filter(i -> i.value().getFkEntity() == 20 && i.partition() == e20Partition).toList();
        // assert that all ipr with fkEntity 20 are in same partition as e with pkEntity 20
        assertEquals(repart20Partitions.size(), 4);

        var e23Partition = e.stream().filter((item) -> item.value().getPkEntity() == 23).findFirst().get().partition();
        var repart23Partitions = iprRepart.stream()
                .filter(i -> i.value().getFkEntity() == 23 && i.partition() == e23Partition).toList();
        assertEquals(repart23Partitions.size(), 2);


        // test stores
        var estore = StoreGetter.getStore(eStore, kafkaStreams);

        var val = estore.get(20);
        // assert that the class of entity 20 is 30
        Assertions.assertEquals(val.getFkClass(), 30);

        val = estore.get(23);
        // assert that the class of entity 23 is 32
        Assertions.assertEquals(val.getFkClass(), 32);

        assertEquals(ep.size(), 6);
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