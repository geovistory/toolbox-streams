/*
 * This Java source file was generated by the Gradle 'init' task.
 */
package org.geovistory.toolbox.streams.statement.subject;


import io.quarkus.runtime.ShutdownEvent;
import org.apache.kafka.streams.Topology;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.lib.TsAdmin;
import org.geovistory.toolbox.streams.statement.subject.processors.StatementSubject;
import org.jboss.logging.Logger;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.Objects;

@ApplicationScoped
public class App {
    private static final Logger LOGGER = Logger.getLogger("ListenerBean");

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
    StatementSubject statementSubject;

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
            var infStatementTable = registerInputTopic.infStatementTable();
            var nodeTable = registerInputTopic.nodeTable();

            // add sub-topology StatementSubject
            statementSubject.addProcessors(
                    infStatementTable,
                    nodeTable
            );
            initialized = true;
        }
    }

    private void createTopics() {
        var admin = new TsAdmin(bootstrapServers);


        // create output topics (with number of partitions and delete.policy=compact)
        var topics = new ArrayList<String>();
        topics.add(statementSubject.outStatementWithSubject());
        if (Objects.equals(createOutputForPostgres, "true")) {
            topics.add(statementSubject.outStatementWithSubjectFlat());
        }
        admin.createOrConfigureTopics(topics, outputTopicPartitions, outputTopicReplicationFactor);

    }

    // Called when the application is terminating
    public void onStop(@Observes ShutdownEvent ev) {
        LOGGER.info("The application is stopping ...");

        // Terminate the container
        // System.exit(0);
    }
}
