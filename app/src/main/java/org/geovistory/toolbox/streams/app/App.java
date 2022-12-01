/*
 * This Java source file was generated by the Gradle 'init' task.
 */
package org.geovistory.toolbox.streams.app;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.geovistory.toolbox.streams.lib.AppConfig;

import java.util.Properties;

class App {
    public static void main(String[] args) {

        StreamsBuilder builder = new StreamsBuilder();
        var a = ProjectProfilesTopology.addProcessors(builder);
        ProjectPropertyTopology.addProcessors(a);
        var topology = a.build();
        Properties props = getConfig();

        // build the topology
        System.out.println("Starting Toolbox Streams App v" + BuildProperties.getDockerTagSuffix());

        // create the streams app
        KafkaStreams streams = new KafkaStreams(topology, props);

        // close Kafka Streams when the JVM shuts down (e.g. SIGTERM)
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        // start streaming!
        streams.start();

    }

    private static Properties getConfig() {

        AppConfig appConfig = AppConfig.INSTANCE;

        // set the required properties for running Kafka Streams
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appConfig.getApplicationId());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, appConfig.getKafkaBootstrapServers());
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);

        props.put(StreamsConfig.STATE_DIR_CONFIG, appConfig.getStateDir());
        /*
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, AvroSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, AvroSerde.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // URL for Apicurio Registry connection (including basic auth parameters)
        props.put(SerdeConfig.REGISTRY_URL, appConfig.getApicurioRegistryUrl());
        // Specify using specific (generated) Avro schema classes
        props.put(AvroKafkaSerdeConfig.USE_SPECIFIC_AVRO_READER, "true");
        props.put(SerdeConfig.AUTO_REGISTER_ARTIFACT, true);*/
        return props;
    }
}
