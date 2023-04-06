/*
 * This Java source file was generated by the Gradle 'init' task.
 */
package org.geovistory.toolbox.streams.base.model;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.geovistory.toolbox.streams.base.model.processors.*;
import org.geovistory.toolbox.streams.lib.Admin;
import org.geovistory.toolbox.streams.lib.AppConfig;

import java.util.ArrayList;

class App {
    public static void main(String[] args) {


        StreamsBuilder builder = new StreamsBuilder();

        var ontomeClass = new OntomeClassProjected(builder);
        var ontomeProperty = new OntomePropertyProjected(builder);

        // add processors of sub-topologies
        addSubTopologies(builder, ontomeClass, ontomeProperty);

        // build the topology
        var topology = builder.build();

        System.out.println(topology.describe());

        // create topics in advance to ensure correct configuration (partition, compaction, ect.)
        createTopics(ontomeClass, ontomeProperty);

        // print configuration information
        System.out.println("Starting Toolbox Streams App v" + BuildProperties.getDockerTagSuffix());
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

    private static void addSubTopologies(
            StreamsBuilder builder,
            OntomeClassProjected ontomeClass,
            OntomePropertyProjected ontomeProperty) {

        ontomeClass.addSink();
        ontomeProperty.addSink();


        // add sub-topology OntomeClassMetadata
        OntomeClassMetadata.addProcessors(builder,
                ontomeClass.kStream
        );
        // add sub-topology OntomeClassLabel
        OntomeClassLabel.addProcessors(builder,
                ontomeClass.kStream
        );
        // add sub-topology OntomePropertyLabel
        OntomePropertyLabel.addProcessors(builder,
                ontomeProperty.kStream
        );
        // add sub-topology HasTypeProperty
        HasTypeProperty.addProcessors(builder,
                ontomeProperty.kStream
        );
    }

    private static void createTopics(
            OntomeClassProjected ontomeClass,
            OntomePropertyProjected ontomeProperty
    ) {
        var admin = new Admin();

        var outputTopicPartitions = Integer.parseInt(AppConfig.INSTANCE.getOutputTopicPartitions());
        var outputTopicReplicationFactor = Short.parseShort(AppConfig.INSTANCE.getOutputTopicReplicationFactor());

        // create output topics (with number of partitions and delete.policy=compact)
        var topics = new ArrayList<String>();
        topics.add(OntomeClassLabel.output.TOPICS.ontome_class_label);
        topics.add(OntomePropertyLabel.output.TOPICS.ontome_property_label);
        topics.add(OntomeClassMetadata.output.TOPICS.ontome_class_metadata);
        topics.add(HasTypeProperty.output.TOPICS.has_type_property);
        topics.add(ontomeClass.outputTopicName);
        topics.add(ontomeProperty.outputTopicName);
        admin.createOrConfigureTopics(topics, outputTopicPartitions, outputTopicReplicationFactor);

    }


}
