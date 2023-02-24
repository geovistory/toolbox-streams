/*
 * This Java source file was generated by the Gradle 'init' task.
 */
package org.geovistory.toolbox.streams.base.config;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Named;
import org.geovistory.toolbox.streams.base.config.processors.*;
import org.geovistory.toolbox.streams.lib.Admin;
import org.geovistory.toolbox.streams.lib.AppConfig;

import static org.geovistory.toolbox.streams.base.config.BuildProperties.getDockerImageTag;
import static org.geovistory.toolbox.streams.base.config.BuildProperties.getDockerTagSuffix;

class App {
    public static void main(String[] args) {


        StreamsBuilder builder = new StreamsBuilder();

        // add processors of sub-topologies
        addSubTopologies(builder);

        // build the topology
        var topology = builder.build();

        System.out.println(topology.describe());

        // create topics in advance to ensure correct configuration (partition, compaction, ect.)
        createTopics();

        // print configuration information
        System.out.println("Starting Toolbox Streams App " + getDockerImageTag() + ":" + getDockerTagSuffix());
        System.out.println("With config:");
        AppConfig.INSTANCE.printConfigs();

        // create the streams app
        // noinspection resource
        KafkaStreams streams = new KafkaStreams(topology, AppConfig.getConfig());

        // close Kafka Streams when the JVM shuts down (e.g. SIGTERM)
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        // start streaming!
        streams.start();
    }

    private static void addSubTopologies(StreamsBuilder builder) {
        var inputTopics = new RegisterInputTopic(builder);
        var innerTopic = new RegisterInnerTopic(builder);

        // register input topics as KTables
        var proProjectTable = inputTopics.proProjectTable();
        var proTextPropertyStream = inputTopics.proTextPropertyStream();
        var proProfileProjRelTable = inputTopics.proProfileProjRelTable();
        var sysConfigTable = inputTopics.sysConfigTable();
        var ontomeClassLabelTable = inputTopics.ontomeClassLabelTable();

        // register input topics as KStreams
        var proEntityLabelConfigStream = inputTopics.proEntityLabelConfigStream();
        var ontomePropertyStream = inputTopics.ontomePropertyStream();
        var ontomeClassStream = inputTopics.ontomeClassStream();
        var ontomePropertyLabelStream = inputTopics.ontomePropertyLabelStream();

        // add sub-topology ProjectProfiles
        var projectProfiles = ProjectProfiles.addProcessors(builder,
                proProjectTable,
                proProfileProjRelTable,
                sysConfigTable);

        // add sub-topology ProjectProperty
        var projectProperty = ProjectProperty.addProcessors(builder,
                ontomePropertyStream,
                projectProfiles.projectProfileStream());

        // add sub-topology ProjectClass
        var projectClass = ProjectClass.addProcessors(builder,
                projectProfiles.projectProfileStream(),
                ontomeClassStream
        );
        var projectClassTable = innerTopic.projectClassTable();


        // add sub-topology GeovClassLabel
        var geovClassLabel = GeovClassLabel.addProcessors(builder,
                proTextPropertyStream
        );

        // add sub-topology ProjectClassLabel
        ProjectClassLabel.addProcessors(builder,
                proProjectTable,
                ontomeClassLabelTable.toStream(Named.as("ktable-ontome-class-label-to-stream")),
                geovClassLabel.geovClassLabelStream(),
                projectClass.projectClassStream()
        );

        // add sub-topology CommunityEntityLabelConfig
        CommunityEntityLabelConfig.addProcessors(builder,
                proEntityLabelConfigStream
        );
        var communityEntityLabelConfigTable = innerTopic.communityEntityLabelConfigTable();

        // add sub-topology ProjectEntityLabelConfig
        ProjectEntityLabelConfig.addProcessors(builder,
                projectClassTable,
                proEntityLabelConfigStream,
                communityEntityLabelConfigTable
        );


        // add sub-topology GeovPropertyLabel
        var geovPropertyLabel = GeovPropertyLabel.addProcessors(builder,
                proTextPropertyStream
        );

        // add sub-topology ProjectPropertyLabel
        ProjectPropertyLabel.addProcessors(builder,
                proProjectTable,
                ontomePropertyLabelStream,
                geovPropertyLabel.geovPropertyLabelStream(),
                projectProperty.projectPropertyStream()
        );

        // add sub-topology CommunityPropertyLabel
        CommunityPropertyLabel.addProcessors(builder,
                ontomePropertyStream,
                geovPropertyLabel.geovPropertyLabelStream()
        );

        // add sub-topology CommunityClassLabel
        CommunityClassLabel.addProcessors(builder,
                ontomeClassLabelTable,
                geovClassLabel.geovClassLabelStream()
        );

    }

    private static void createTopics() {
        var admin = new Admin();

        var outputTopicPartitions = Integer.parseInt(AppConfig.INSTANCE.getOutputTopicPartitions());
        var outputTopicReplicationFactor = Short.parseShort(AppConfig.INSTANCE.getOutputTopicReplicationFactor());

        // create output topics (with number of partitions and delete.policy=compact)
        admin.createOrConfigureTopics(new String[]{
                GeovClassLabel.output.TOPICS.geov_class_label,
                ProjectClass.output.TOPICS.project_class,
                ProjectProfiles.output.TOPICS.project_profile,
                ProjectProperty.output.TOPICS.project_property,
                ProjectClassLabel.output.TOPICS.project_class_label,
                CommunityEntityLabelConfig.output.TOPICS.community_entity_label_config,
                CommunityClassLabel.output.TOPICS.community_class_label,
                CommunityPropertyLabel.output.TOPICS.community_property_label,
                GeovPropertyLabel.output.TOPICS.geov_property_label,
                ProjectPropertyLabel.output.TOPICS.project_property_label,
                ProjectEntityLabelConfig.output.TOPICS.project_entity_label_config,
        }, outputTopicPartitions, outputTopicReplicationFactor);
    }


}
