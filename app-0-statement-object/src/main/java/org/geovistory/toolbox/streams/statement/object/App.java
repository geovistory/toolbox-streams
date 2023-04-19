/*
 * This Java source file was generated by the Gradle 'init' task.
 */
package org.geovistory.toolbox.streams.statement.object;

import org.apache.kafka.streams.Topology;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.lib.TsAdmin;
import org.geovistory.toolbox.streams.statement.object.processors.StatementObject;

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
    BuilderSingleton builderSingleton;

    @Inject
    RegisterInputTopic registerInputTopic;

    @Inject
    StatementObject statementObject;

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

            // register input topics as KTables
            var statementWithSubjectTable = registerInputTopic.statementWithSubjectTable();
            var nodeTable = registerInputTopic.nodeTable();

            // add sub-topology StatementEnriched
            statementObject.addProcessors(
                    statementWithSubjectTable,
                    nodeTable
            );
            initialized = true;
        }
    }

    private void createTopics() {
        var admin = new TsAdmin(bootstrapServers);

        var topics = new ArrayList<String>();
        topics.add(statementObject.outStatementWithEntity());
        topics.add(statementObject.outStatementWithLiteral());
        topics.add(statementObject.outStatementOther());
        if (Objects.equals(createOutputForPostgres, "true")) {
            topics.add(statementObject.outStatementEnrichedFlat());
        }
        // create output topics (with number of partitions and delete.policy=compact)
        admin.createOrConfigureTopics(topics, outputTopicPartitions, outputTopicReplicationFactor);

    }


}
