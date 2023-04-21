/*
 * This Java source file was generated by the Gradle 'init' task.
 */
package org.geovistory.toolbox.streams.base.config;

import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Named;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.base.config.processors.*;
import org.geovistory.toolbox.streams.lib.TsAdmin;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import java.util.ArrayList;

@ApplicationScoped
public class App {

    @ConfigProperty(name = "ts.output.topic.partitions")
    int outputTopicPartitions;
    @ConfigProperty(name = "ts.output.topic.replication.factor")
    short outputTopicReplicationFactor;

    @ConfigProperty(name = "quarkus.kafka.streams.bootstrap.servers")
    String bootstrapServers;

    @Inject
    ProjectProfiles projectProfiles;
    @Inject
    ProjectProperty projectProperty;
    @Inject
    ProjectClass projectClass;
    @Inject
    GeovClassLabel geovClassLabel;
    @Inject
    ProjectClassLabel projectClassLabel;
    @Inject
    CommunityEntityLabelConfig communityEntityLabelConfig;
    @Inject
    ProjectEntityLabelConfig projectEntityLabelConfig;
    @Inject
    GeovPropertyLabel geovPropertyLabel;
    @Inject
    ProjectPropertyLabel projectPropertyLabel;
    @Inject
    CommunityPropertyLabel communityPropertyLabel;
    @Inject
    CommunityClassLabel communityClassLabel;
    @Inject
    BuilderSingleton builderSingleton;
    @Inject
    RegisterInputTopic registerInputTopic;
    @Inject
    RegisterInnerTopic registerInnerTopic;

    @Inject
    OutputTopicNames outputTopicNames;
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
            var proProjectTable = registerInputTopic.proProjectTable();
            var proTextPropertyStream = registerInputTopic.proTextPropertyStream();
            var proProfileProjRelTable = registerInputTopic.proProfileProjRelTable();
            var sysConfigStream = registerInputTopic.sysConfigStream();
            var ontomeClassLabelTable = registerInputTopic.ontomeClassLabelTable();

            // register input topics as KStreams
            var proEntityLabelConfigStream = registerInputTopic.proEntityLabelConfigStream();
            var ontomePropertyStream = registerInputTopic.ontomePropertyStream();
            var ontomeClassStream = registerInputTopic.ontomeClassStream();
            var ontomePropertyLabelStream = registerInputTopic.ontomePropertyLabelStream();


            // add sub-topology ProjectProfiles
            var projectProfilesReturn = projectProfiles.addProcessors(
                    proProjectTable,
                    proProfileProjRelTable,
                    sysConfigStream);

            // add sub-topology ProjectProperty
            var projectPropertyReturn = projectProperty.addProcessors(
                    ontomePropertyStream,
                    projectProfilesReturn.projectProfileStream());

            // add sub-topology ProjectClass
            var projectClassReturn = projectClass.addProcessors(
                    projectProfilesReturn.projectProfileStream(),
                    ontomeClassStream
            );
            var projectClassTable = registerInnerTopic.projectClassTable();


            // add sub-topology GeovClassLabel
            var geovClassLabelReturn = geovClassLabel.addProcessors(
                    proTextPropertyStream
            );

            // add sub-topology ProjectClassLabel
            projectClassLabel.addProcessors(
                    proProjectTable,
                    ontomeClassLabelTable.toStream(Named.as("ktable-ontome-class-label-to-stream")),
                    geovClassLabelReturn.geovClassLabelStream(),
                    projectClassReturn.projectClassStream()
            );

            // add sub-topology CommunityEntityLabelConfig
            communityEntityLabelConfig.addProcessors(
                    proEntityLabelConfigStream
            );
            var communityEntityLabelConfigTable = registerInnerTopic.communityEntityLabelConfigTable();

            // add sub-topology ProjectEntityLabelConfig
            projectEntityLabelConfig.addProcessors(
                    projectClassTable,
                    proEntityLabelConfigStream,
                    communityEntityLabelConfigTable
            );


            // add sub-topology GeovPropertyLabel
            var geovPropertyLabelReturn = geovPropertyLabel.addProcessors(
                    proTextPropertyStream
            );

            // add sub-topology ProjectPropertyLabel
            projectPropertyLabel.addProcessors(
                    proProjectTable,
                    ontomePropertyLabelStream,
                    geovPropertyLabelReturn.geovPropertyLabelStream(),
                    projectPropertyReturn.projectPropertyStream()
            );

            // add sub-topology CommunityPropertyLabel
            communityPropertyLabel.addProcessors(
                    ontomePropertyStream,
                    geovPropertyLabelReturn.geovPropertyLabelStream()
            );

            // add sub-topology CommunityClassLabel
            communityClassLabel.addProcessors(
                    ontomeClassLabelTable,
                    geovClassLabelReturn.geovClassLabelStream()
            );

        }
    }

    private void createTopics() {
        var admin = new TsAdmin(bootstrapServers);

        // create output topics (with number of partitions and delete.policy=compact)
        var topics = new ArrayList<String>();
        topics.add(outputTopicNames.geovClassLabel());
        topics.add(outputTopicNames.projectClass());
        topics.add(outputTopicNames.projectProfile());
        topics.add(outputTopicNames.projectProperty());
        topics.add(outputTopicNames.projectClassLabel());
        topics.add(outputTopicNames.communityEntityLabelConfig());
        topics.add(outputTopicNames.communityClassLabel());
        topics.add(outputTopicNames.communityPropertyLabel());
        topics.add(outputTopicNames.geovPropertyLabel());
        topics.add(outputTopicNames.projectPropertyLabel());
        topics.add(outputTopicNames.projectEntityLabelConfig());

        admin.createOrConfigureTopics(topics, outputTopicPartitions, outputTopicReplicationFactor);
    }


}
