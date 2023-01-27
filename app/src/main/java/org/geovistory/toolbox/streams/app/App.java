/*
 * This Java source file was generated by the Gradle 'init' task.
 */
package org.geovistory.toolbox.streams.app;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.geovistory.toolbox.streams.lib.AppConfig;
import org.geovistory.toolbox.streams.topologies.*;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

class App {
    public static void main(String[] args) {

        // create topics in advance to ensure correct configuration (partition, compaction, ect.)
        createTopics();

        StreamsBuilder builder = new StreamsBuilder();

        // add processors of sub-topologies
        addSubTopologies(builder);

        // build the topology
        var topology = builder.build();

        // print configuration information
        System.out.println("Starting Toolbox Streams App v" + BuildProperties.getDockerTagSuffix());
        System.out.println("With config:");
        AppConfig.INSTANCE.printConfigs();

        // create the streams app
        // noinspection resource
        KafkaStreams streams = new KafkaStreams(topology, getConfig());

        // close Kafka Streams when the JVM shuts down (e.g. SIGTERM)
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        // start streaming!
        streams.start();
    }

    private static void addSubTopologies(StreamsBuilder builder) {
        var inputTopics = new RegisterInputTopic(builder);
        var outputTopics = new RegisterOutputTopic(builder);

        // register input topics as KTables
        var proProjectTable = inputTopics.proProjectTable();
        var proTextPropertyTable = inputTopics.proTextPropertyTable();
        var proProfileProjRelTable = inputTopics.proProfileProjRelTable();
        var proInfoProjRelTable = inputTopics.proInfoProjRelTable();
        var infResourceTable = inputTopics.infResourceTable();
        var infStatementTable = inputTopics.infStatementTable();
        var infLanguageStream = inputTopics.infLanguageStream();
        var infAppellationStream = inputTopics.infAppellationStream();
        var infLangStringStream = inputTopics.infLangStringStream();
        var infPlaceStream = inputTopics.infPlaceStream();
        var infTimePrimitiveStream = inputTopics.infTimePrimitiveStream();
        var infDimensionStream = inputTopics.infDimensionStream();
        var datDigitalStream = inputTopics.datDigitalStream();
        var tabCellStream = inputTopics.tabCellStream();
        var dfhApiClassTable = inputTopics.dfhApiClassTable();
        var dfhApiPropertyTable = inputTopics.dfhApiPropertyTable();
        var sysConfigTable = inputTopics.sysConfigTable();

        // register input topics as KStreams
        var proEntityLabelConfigStream = inputTopics.proEntityLabelConfigStream();

        // register recursive output topics as KTables
        var projectEntityLabelTable = outputTopics.projectEntityLabelTable();
        var projectPropertyLabelTable = outputTopics.projectPropertyLabelTable();
        var projectEntityTopStatementsTable = outputTopics.projectEntityTopStatementsTable();

        // add sub-topology ProjectProfiles
        var projectProfiles = ProjectProfiles.addProcessors(builder,
                proProjectTable,
                proProfileProjRelTable,
                sysConfigTable);

        // add sub-topology ProjectProperty
        var projectProperty = ProjectProperty.addProcessors(builder,
                dfhApiPropertyTable,
                projectProfiles.projectProfileStream());

        // add sub-topology ProjectClass
        var projectClass = ProjectClass.addProcessors(builder,
                projectProfiles.projectProfileStream(),
                dfhApiClassTable
        );
        var projectClassTable = outputTopics.projectClassTable();

        // add sub-topology OntomeClassLabel
        var ontomeClassLabel = OntomeClassLabel.addProcessors(builder,
                dfhApiClassTable
        );

        // add sub-topology GeovClassLabel
        var geovClassLabel = GeovClassLabel.addProcessors(builder,
                proTextPropertyTable
        );

        // add sub-topology ProjectClassLabel
        ProjectClassLabel.addProcessors(builder,
                proProjectTable,
                ontomeClassLabel.ontomeClassLabelStream(),
                geovClassLabel.geovClassLabelStream(),
                projectClass.projectClassStream()
        );

        // add sub-topology CommunityEntityLabelConfig
        CommunityEntityLabelConfig.addProcessors(builder,
                proEntityLabelConfigStream
        );
        var communityEntityLabelConfigTable = outputTopics.communityEntityLabelConfigTable();

        // add sub-topology ProjectEntityLabelConfig
        ProjectEntityLabelConfig.addProcessors(builder,
                projectClassTable,
                proEntityLabelConfigStream,
                communityEntityLabelConfigTable
        );
        var projectEntityLabelConfigTable = outputTopics.projectEntityLabelConfigTable();

        // add sub-topology StatementEnriched
        var statementEnriched = StatementEnriched.addProcessors(builder,
                infStatementTable,
                infLanguageStream,
                infAppellationStream,
                infLangStringStream,
                infPlaceStream,
                infTimePrimitiveStream,
                infDimensionStream,
                datDigitalStream,
                tabCellStream
        );

        // add sub-topology ProjectStatement
        ProjectStatement.addProcessors(builder,
                statementEnriched.statementEnrichedTable(),
                proInfoProjRelTable,
                projectEntityLabelTable
        );
        var projectStatementTable = outputTopics.projectStatementTable();

        // add sub-topology ProjectTopIncomingStatements
        var projectTopIncomingStatements = ProjectTopIncomingStatements.addProcessors(builder,
                projectStatementTable
        );

        // add sub-topology ProjectTopOutgoingStatements
        var projectTopOutgoingStatements = ProjectTopOutgoingStatements.addProcessors(builder,
                projectStatementTable
        );

        // add sub-topology ProjectTopStatements
        var projectTopStatements = ProjectTopStatements.addProcessors(builder,
                projectTopOutgoingStatements.projectTopStatementStream(),
                projectTopIncomingStatements.projectTopStatementStream()
        );

        // add sub-topology ProjectEntity
        ProjectEntity.addProcessors(builder,
                infResourceTable,
                proInfoProjRelTable
        );
        var projectEntityTable = outputTopics.projectEntityTable();

        // add sub-topology ProjectEntityLabel
        ProjectEntityLabel.addProcessors(builder,
                projectEntityTable,
                projectEntityLabelConfigTable,
                projectTopStatements.projectTopStatementTable()
        );

        // add sub-topology OntomePropertyLabel
        var ontomePropertyLabel = OntomePropertyLabel.addProcessors(builder,
                dfhApiPropertyTable
        );


        // add sub-topology GeovPropertyLabel
        var geovPropertyLabel = GeovPropertyLabel.addProcessors(builder,
                proTextPropertyTable
        );

        // add sub-topology ProjectPropertyLabel
        ProjectPropertyLabel.addProcessors(builder,
                proProjectTable,
                ontomePropertyLabel.ontomePropertyLabelStream(),
                geovPropertyLabel.geovPropertyLabelStream(),
                projectProperty.projectPropertyStream()
        );
        // add sub-topology ProjectEntityTopStatements
        var projectEntityTopStatements = ProjectEntityTopStatements.addProcessors(builder,
                projectEntityTable,
                projectTopStatements.projectTopStatementTable(),
                projectPropertyLabelTable
        );

        // add sub-topology ProjectEntityFulltext
        ProjectEntityFulltext.addProcessors(builder,
                projectEntityTopStatementsTable,
                projectEntityLabelConfigTable
        );

        // add sub-topology ProjectEntityTimeSpan
        ProjectEntityTimeSpan.addProcessors(builder,
                projectEntityTopStatements.projectEntityTopStatementStream()
        );

        // add sub-topology HasTypeProperty
        HasTypeProperty.addProcessors(builder,
                inputTopics.dfhApiPropertyStream()
        );

        // add sub-topology ProjectEntityType
        ProjectEntityType.addProcessors(builder,
                projectEntityTable,
                outputTopics.hasTypePropertyTable(),
                projectTopOutgoingStatements.projectTopStatementTable()
        );

    }

    private static void createTopics() {
        var admin = new Admin();

        var outputTopicPartitions = Integer.parseInt(AppConfig.INSTANCE.getOutputTopicPartitions());
        var outputTopicReplicationFactor = Short.parseShort(AppConfig.INSTANCE.getOutputTopicReplicationFactor());

        // create output topics (with number of partitions and delete.policy=compact)
        var kafkaFuture = admin.createTopics(new String[]{
                OntomeClassLabel.output.TOPICS.ontome_class_label,
                GeovClassLabel.output.TOPICS.geov_class_label,
                ProjectClass.output.TOPICS.project_class,
                ProjectProfiles.output.TOPICS.project_profile,
                ProjectProperty.output.TOPICS.project_property,
                ProjectEntity.output.TOPICS.project_entity,
                ProjectClassLabel.output.TOPICS.project_class_label,
                StatementEnriched.output.TOPICS.statement_enriched,
                ProjectEntityLabelConfig.output.TOPICS.project_entity_label_config_enriched,
                CommunityEntityLabelConfig.output.TOPICS.community_entity_label_config,
                ProjectStatement.output.TOPICS.project_statement,
                ProjectTopOutgoingStatements.output.TOPICS.project_top_outgoing_statements,
                ProjectTopIncomingStatements.output.TOPICS.project_top_incoming_statements,
                ProjectTopStatements.output.TOPICS.project_top_statements,
                ProjectEntityLabel.output.TOPICS.project_entity_label,
                OntomePropertyLabel.output.TOPICS.ontome_property_label,
                GeovPropertyLabel.output.TOPICS.geov_property_label,
                ProjectPropertyLabel.output.TOPICS.project_property_label,
                ProjectEntityTopStatements.output.TOPICS.project_entity_top_statements,
                ProjectEntityFulltext.output.TOPICS.project_entity_fulltext_label,
                ProjectEntityTimeSpan.output.TOPICS.project_entity_time_span,
                HasTypeProperty.output.TOPICS.has_type_property,
                ProjectEntityType.output.TOPICS.project_entity_type
        }, outputTopicPartitions, outputTopicReplicationFactor);


        // Use the KafkaFuture object to block and wait for the topic creation to complete
        try {
            kafkaFuture.get();
            System.out.println("Topics created successfully");
        } catch (InterruptedException | ExecutionException e) {
            System.out.println("Error creating topics: " + e.getMessage());
        }
    }

    private static Properties getConfig() {

        AppConfig appConfig = AppConfig.INSTANCE;

        // set the required properties for running Kafka Streams
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appConfig.getApplicationId());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, appConfig.getKafkaBootstrapServers());
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);

        props.put(StreamsConfig.STATE_DIR_CONFIG, appConfig.getStateDir());

        props.put(StreamsConfig.topicPrefix(TopicConfig.CLEANUP_POLICY_CONFIG), TopicConfig.CLEANUP_POLICY_COMPACT);

        // See this for producer configs:
        // https://docs.confluent.io/platform/current/streams/developer-guide/config-streams.html#ak-consumers-producer-and-admin-client-configuration-parameters
        props.put(StreamsConfig.producerPrefix(ProducerConfig.MAX_REQUEST_SIZE_CONFIG), "20971760");

        // rocksdb memory management
        // see https://medium.com/@grinfeld_433/kafka-streams-and-rocksdb-in-the-space-time-continuum-and-a-little-bit-of-configuration-40edb5ee9ed7
        // see https://kafka.apache.org/33/documentation/streams/developer-guide/memory-mgmt.html#id3
        props.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, BoundedMemoryRocksDBConfig.class.getName());
        props.put(BoundedMemoryRocksDBConfig.TOTAL_OFF_HEAP_SIZE_MB, appConfig.getRocksdbTotalOffHeapMb());
        props.put(BoundedMemoryRocksDBConfig.TOTAL_MEMTABLE_MB, appConfig.getRocksdbTotalMemtableMb());

        // streams memory management
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, appConfig.getStreamsCacheMaxBytesBuffering());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, appConfig.getStreamsCommitIntervalMs());
        props.put(StreamsConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIG, appConfig.getStreamsBufferedRecordsPerPartition());

        // producer memory management
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, appConfig.getStreamsBufferMemory());
        props.put(ProducerConfig.SEND_BUFFER_CONFIG, appConfig.getStreamsSendBufferBytes());

        // consumer memory management
        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, appConfig.getStreamsFetchMaxBytes());
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, appConfig.getStreamsFetchMaxWaitMs());
        props.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, appConfig.getStreamsReceiveBufferBytes());


        /*

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
