/*
 * This Java source file was generated by the Gradle 'init' task.
 */
package org.geovistory.toolbox.streams.nodes;

import org.apache.kafka.streams.Topology;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.lib.TsAdmin;
import org.geovistory.toolbox.streams.nodes.processors.Nodes;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Objects;

@ApplicationScoped
public class App {

    @ConfigProperty(name = "ts.output.topic.partitions")
    int outputTopicPartitions;
    @ConfigProperty(name = "ts.output.topic.replication.factor")
    short outputTopicReplicationFactor;

    @ConfigProperty(name = "quarkus.kafka.streams.bootstrap.servers")
    String bootstrapServers;

    @ConfigProperty(name = "create.output.for.postgres", defaultValue = "false")
    public String createOutputForPostgres;

    @Inject
    Nodes nodes;

    @Inject
    BuilderSingleton builderSingleton;

    @Inject
    RegisterInputTopic registerInputTopic;

    Boolean initialized = false;

    //  All we need to do for that is to declare a CDI producer method which returns the Kafka Streams Topology; the Quarkus extension will take care of configuring, starting and stopping the actual Kafka Streams engine.
    @Produces
    public Topology buildTopology() {

        // add processors of sub-topologies
        addSubTopologies();

        // create topics in advance to ensure correct configuration (partition, compaction, ect.)
        createTopics();

        // build the topology
        return builderSingleton.builder.build();
    }


    private void addSubTopologies() {

        if (!initialized) {
            initialized = true;
            // register input topics as KTables
            var infResourceStream = registerInputTopic.infResourceStream();
            var infLanguageStream = registerInputTopic.infLanguageStream();
            var infAppellationStream = registerInputTopic.infAppellationStream();
            var infLangStringStream = registerInputTopic.infLangStringStream();
            var infPlaceStream = registerInputTopic.infPlaceStream();
            var infTimePrimitiveStream = registerInputTopic.infTimePrimitiveStream();
            var infDimensionStream = registerInputTopic.infDimensionStream();
            var datDigitalStream = registerInputTopic.datDigitalStream();
            var tabCellStream = registerInputTopic.tabCellStream();

            // add sub-topology StatementEnriched
            nodes.addProcessors(
                    infResourceStream,
                    infLanguageStream,
                    infAppellationStream,
                    infLangStringStream,
                    infPlaceStream,
                    infTimePrimitiveStream,
                    infDimensionStream,
                    datDigitalStream,
                    tabCellStream
            );
        }
    }

    private void createTopics() {
        var admin = new TsAdmin(bootstrapServers);

        // create output topics (with number of partitions and delete.policy=compact)
        var topics = new ArrayList<String>();
        topics.add(nodes.outNodes());
        if (Objects.equals(createOutputForPostgres, "true")) {
            topics.add(nodes.outNodesFlat());
        }
        admin.createOrConfigureTopics(topics, outputTopicPartitions, outputTopicReplicationFactor);


    }


}
