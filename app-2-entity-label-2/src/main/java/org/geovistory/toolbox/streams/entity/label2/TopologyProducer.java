package org.geovistory.toolbox.streams.entity.label2;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import org.apache.kafka.streams.Topology;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.lib.TsAdmin;

import java.util.ArrayList;

@ApplicationScoped
public class TopologyProducer {
    @ConfigProperty(name = "kafka.bootstrap.servers")
    String bootstrapServers;
    @ConfigProperty(name = "ts.output.topic.partitions")
    int outputTopicPartitions;
    @ConfigProperty(name = "ts.output.topic.replication.factor")
    short outputTopicReplicationFactor;

    @Inject
    InputTopicNames inputTopicNames;

    @Inject
    ConfiguredAvroSerde as;

    @Produces
    public Topology buildTopology() {

        // create topics in advance to ensure correct configuration (partition, compaction, ect.)
        createTopics();

        return new Topology()
                .addSource(
                        inputTopicNames.infResource() + "-source",
                        as.<ts.information.resource.Key>key().deserializer(),
                        as.<ts.information.resource.Value>value().deserializer(),
                        inputTopicNames.infResource()
                )
                .addProcessor(
                        "test-process",
                        TestProcessor::new,
                        inputTopicNames.infResource() + "-source")
                .addSink(
                        "test-sink",
                        "test",
                        as.<ts.information.resource.Key>key().serializer(),
                        as.<ts.information.resource.Value>value().serializer(),
                        "test-process"
                );


    }

    private void createTopics() {
        var admin = new TsAdmin(bootstrapServers);
        // create output topics (with number of partitions and delete.policy=compact)
        var topics = new ArrayList<String>();
        topics.add("test");
        admin.createOrConfigureTopics(topics, outputTopicPartitions, outputTopicReplicationFactor);
    }
}

