/*
 * This Java source file was generated by the Gradle 'init' task.
 */
package org.geovistory.toolbox.streams.entity.preview;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.geovistory.toolbox.streams.entity.preview.processors.EntityPreview;
import org.geovistory.toolbox.streams.entity.preview.processors.community.CommunityEntityPreview;
import org.geovistory.toolbox.streams.entity.preview.processors.project.ProjectEntityPreview;
import org.geovistory.toolbox.streams.lib.Admin;
import org.geovistory.toolbox.streams.lib.AppConfig;

import static org.geovistory.toolbox.streams.entity.preview.BuildProperties.getDockerImageTag;
import static org.geovistory.toolbox.streams.entity.preview.BuildProperties.getDockerTagSuffix;

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

        addProjectView(builder, inputTopics);
        addCommunityView(builder, inputTopics, "toolbox");
        addMergedView(builder, "toolbox");
    }

    private static void addProjectView(StreamsBuilder builder, RegisterInputTopic inputTopics) {
        // register input topics as KTables
        var projectEntityTable = inputTopics.projectEntityTable();
        var projectEntityLabelTable = inputTopics.projectEntityLabelTable();
        var projectEntityClassLabelTable = inputTopics.projectEntityClassLabelTable();
        var projectEntityTypeTable = inputTopics.projectEntityTypeTable();
        var projectEntityTimeSpanTable = inputTopics.projectEntityTimeSpanTable();
        var projectEntityFulltextTable = inputTopics.projectEntityFulltextTable();
        var projectEntityClassMetadataTable = inputTopics.projectEntityClassMetadataTable();

        // add sub-topology ProjectEntityPreview
        ProjectEntityPreview.addProcessors(builder,
                projectEntityTable,
                projectEntityLabelTable,
                projectEntityClassLabelTable,
                projectEntityTypeTable,
                projectEntityTimeSpanTable,
                projectEntityFulltextTable,
                projectEntityClassMetadataTable
        );
    }

    private static void addCommunityView(StreamsBuilder builder, RegisterInputTopic inputTopics, String nameSupplement) {
        // register input topics as KTables
        var communityEntityTable = inputTopics.communityEntityTable();
        var communityEntityLabelTable = inputTopics.communityEntityLabelTable();
        var communityEntityClassLabelTable = inputTopics.communityEntityClassLabelTable();
        var communityEntityTypeTable = inputTopics.communityEntityTypeTable();
        var communityEntityTimeSpanTable = inputTopics.communityEntityTimeSpanTable();
        var communityEntityFulltextTable = inputTopics.communityEntityFulltextTable();
        var communityEntityClassMetadataTable = inputTopics.communityEntityClassMetadataTable();

        // add sub-topology ProjectEntityPreview
        CommunityEntityPreview.addProcessors(builder,
                communityEntityTable,
                communityEntityLabelTable,
                communityEntityClassLabelTable,
                communityEntityTypeTable,
                communityEntityTimeSpanTable,
                communityEntityFulltextTable,
                communityEntityClassMetadataTable,
                nameSupplement
        );
    }

    private static void addMergedView(StreamsBuilder builder, String nameSupplement) {
        var innerTopics = new RegisterInnerTopic(builder);
        // register inner topics as KStream
        var projectEntityPreviewStream = innerTopics.projectEntityPreviewStream();
        var communityEntityPreviewStream = innerTopics.communityEntityPreviewStream(nameSupplement);

        // add sub-topology ProjectEntityPreview
        EntityPreview.addProcessors(builder,
                projectEntityPreviewStream,
                communityEntityPreviewStream
        );
    }

    private static void createTopics() {
        var admin = new Admin();

        var outputTopicPartitions = Integer.parseInt(AppConfig.INSTANCE.getOutputTopicPartitions());
        var outputTopicReplicationFactor = Short.parseShort(AppConfig.INSTANCE.getOutputTopicReplicationFactor());

        // create output topics (with number of partitions and delete.policy=compact)
        admin.createOrConfigureTopics(new String[]{
                ProjectEntityPreview.output.TOPICS.project_entity_preview,
                CommunityEntityPreview.getOutputTopicName("toolbox"),
                EntityPreview.output.TOPICS.entity_preview
        }, outputTopicPartitions, outputTopicReplicationFactor);
    }

}
