package org.geovistory.toolbox.streams.lib;

import io.github.cdimascio.dotenv.Dotenv;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.streams.StreamsConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


public enum AppConfig {
    INSTANCE();
    private final Dotenv dotenv = Dotenv
            .configure()
            .ignoreIfMissing()
            .load();
    private final Map<String, String> configMap = new HashMap<>();

    // url of the apicurio registry
    private final String apicurioRegistryUrl = parseEnv(
            "TS_APICURIO_REGISTRY_URL",
            "http:" +
                    "//localhost:8080/apis/registry/v2");

    // url of the confluent schema registry
    private String schemaRegistryUrl = parseEnv(
            "TS_SCHEMA_REGISTRY_URL",
            "http:" +
                    "//localhost:8081/");

    // kafka bootstrap servers (comma separated)
    private final String kafkaBootstrapServers = parseEnv(
            "TS_BOOTSTRAP_SERVERS",
            "http:" +
                    "//localhost:9092");

    // application id
    private final String applicationId = parseEnv(
            "TS_APPLICATION_ID",
            "dev"
    );

    // path of state store directory
    private final String stateDir = parseEnv(
            "TS_STATE_DIR",
            "tmp/" +
                    "kafka-streams");

    //prefix of input topics (generated by Debezium connector)
    private final String inputTopicPrefix = parseEnv(
            "TS_INPUT_TOPIC_NAME_PREFIX",
            "dev"
    );

    // prefix of output topics (generated by this app)
    private final String outputTopicPrefix = parseEnv(
            "TS_OUTPUT_TOPIC_NAME_PREFIX",
            "dev"
    );

    // Number of partitions of output topics
    private final String outputTopicPartitions = parseEnv(
            "TS_OUTPUT_TOPIC_PARTITIONS",
            "4"
    );

    // Replication factor of output topics
    private final String outputTopicReplicationFactor = parseEnv(
            "TS_OUTPUT_TOPIC_REPLICATION_FACTOR",
            "3"
    );

    // rocksdb TOTAL_OFF_HEAP_SIZE_MB
    private final String rocksdbTotalOffHeapMb = parseEnv(
            "ROCKSDB_TOTAL_OFF_HEAP_MB",
            "1000"
    );


    // rocksdb TOTAL_MEMTABLE_MB
    private final String rocksdbTotalMemtableMb = parseEnv(
            "ROCKSDB_TOTAL_MEMTABLE_MB",
            "100"
    );

    // streams config cache.max.bytes.buffering
    private final String streamsCacheMaxBytesBuffering = parseEnv(
            "STREAMS_CACHE_MAX_BYTES_BUFFERING_CONFIG",
            "10485760"
    ); // 10485760 => 10MB per Thread

    // streams config commit.interval.ms
    private final String streamsCommitIntervalMs = parseEnv(
            "STREAMS_COMMIT_INTERVAL_MS",
            "30000"
    ); // 30000 => 30 seconds

    // streams config buffer.memory
    private final String streamsBufferMemory = parseEnv(
            "STREAMS_BUFFER_MEMORY",
            "33554432"
    ); // 33554432 => 32 MB

    // streams config fetch.max.bytes
    private final String streamsFetchMaxBytes = parseEnv(
            "STREAMS_FETCH_MAX_BYTES",
            "52428800"
    ); // 52428800 => 50 MB

    // streams config fetch.max.wait.ms
    private final String streamsFetchMaxWaitMs = parseEnv(
            "STREAMS_FETCH_MAX_WAIT_MS",
            "500"
    ); // 500 => half a second

    // streams config send.buffer.bytes
    private final String streamsSendBufferBytes = parseEnv(
            "STREAMS_SEND_BUFFER_BYTES",
            "131072"
    ); // 131072 => 128 KB

    // streams config receive.buffer.bytes
    private final String streamsReceiveBufferBytes = parseEnv(
            "STREAMS_RECEIVE_BUFFER_BYTES",
            "32768"
    ); // 32768 => 32 KB

    // streams config buffered.records.per.partition
    private final String streamsBufferedRecordsPerPartition = parseEnv(
            "STREAMS_BUFFERED_RECORDS_PER_PARTITION",
            "1000"
    ); // 1000 records

    private final String metricsRecordingLevel = parseEnv(
            "STREAMS_METRICS_RECORDING_LEVEL_CONFIG",
            "INFO"
    ); // 1000 records

    private final String producerInterceptorClassesConfig = parseEnv(
            "STREAMS_PRODUCER_INTERCEPTOR_CLASSES_CONFIG",
            ""
    );
    private final String consumerInterceptorClassesConfig = parseEnv(
            "STREAMS_CONSUMER_INTERCEPTOR_CLASSES_CONFIG",
            ""
    );

    private final String streamsProcessingGuaranteeConfig = parseEnv(
            "STREAMS_PROCESSING_GUARANTEE_CONFIG",
            "exactly_once_v2"
    );

    private final String streamsConsumerIsolationLevelConfig = parseEnv(
            "STREAMS_CONSUMER_ISOLATION_LEVEL_CONFIG",
            "read_committed"
    );


    AppConfig() {
    }

    public AppConfig getInstance() {
        return INSTANCE;
    }

    public String getApicurioRegistryUrl() {
        return apicurioRegistryUrl;
    }

    public String getKafkaBootstrapServers() {
        return kafkaBootstrapServers;
    }

    public String getApplicationId() {
        return applicationId;
    }

    public String getInputTopicPrefix() {
        return inputTopicPrefix;
    }

    public String getOutputTopicPrefix() {
        return outputTopicPrefix;
    }

    public String getOutputTopicPartitions() {
        return outputTopicPartitions;
    }

    public String getOutputTopicReplicationFactor() {
        return outputTopicReplicationFactor;
    }

    public String getSchemaRegistryUrl() {
        return schemaRegistryUrl;
    }

    public void setSchemaRegistryUrl(String schemaRegistryUrl) {
        this.schemaRegistryUrl = schemaRegistryUrl;
    }


    public String getRocksdbTotalOffHeapMb() {
        return rocksdbTotalOffHeapMb;
    }

    public String getRocksdbTotalMemtableMb() {
        return rocksdbTotalMemtableMb;
    }

    public String getStreamsCacheMaxBytesBuffering() {
        return streamsCacheMaxBytesBuffering;
    }

    public String getStreamsCommitIntervalMs() {
        return streamsCommitIntervalMs;
    }

    public String getStreamsBufferMemory() {
        return streamsBufferMemory;
    }

    public String getStreamsFetchMaxBytes() {
        return streamsFetchMaxBytes;
    }

    public String getStreamsFetchMaxWaitMs() {
        return streamsFetchMaxWaitMs;
    }

    public String getStreamsSendBufferBytes() {
        return streamsSendBufferBytes;
    }

    public String getStreamsReceiveBufferBytes() {
        return streamsReceiveBufferBytes;
    }

    public String getStreamsBufferedRecordsPerPartition() {
        return streamsBufferedRecordsPerPartition;
    }

    public void printConfigs() {
        configMap.forEach((key, val) -> System.out.println(key + ": " + val));
    }


    public static Properties getConfig() {

        // set the required properties for running Kafka Streams
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, INSTANCE.getApplicationId());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, INSTANCE.getKafkaBootstrapServers());
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, INSTANCE.streamsProcessingGuaranteeConfig);

        props.put(StreamsConfig.STATE_DIR_CONFIG, INSTANCE.stateDir);


        props.put(StreamsConfig.topicPrefix(TopicConfig.CLEANUP_POLICY_CONFIG), TopicConfig.CLEANUP_POLICY_COMPACT);

        // See this for producer configs:
        // https://docs.confluent.io/platform/current/streams/developer-guide/config-streams.html#ak-consumers-producer-and-admin-client-configuration-parameters
        props.put(StreamsConfig.producerPrefix(ProducerConfig.MAX_REQUEST_SIZE_CONFIG), "20971760");

        // consumer config
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.ISOLATION_LEVEL_CONFIG), INSTANCE.streamsConsumerIsolationLevelConfig);

        // rocksdb memory management
        // see https://medium.com/@grinfeld_433/kafka-streams-and-rocksdb-in-the-space-time-continuum-and-a-little-bit-of-configuration-40edb5ee9ed7
        // see https://kafka.apache.org/33/documentation/streams/developer-guide/memory-mgmt.html#id3
        props.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, BoundedMemoryRocksDBConfig.class.getName());
        props.put(BoundedMemoryRocksDBConfig.TOTAL_OFF_HEAP_SIZE_MB, INSTANCE.getRocksdbTotalOffHeapMb());
        props.put(BoundedMemoryRocksDBConfig.TOTAL_MEMTABLE_MB, INSTANCE.getRocksdbTotalMemtableMb());

        // streams memory management
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, INSTANCE.getStreamsCacheMaxBytesBuffering());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, INSTANCE.getStreamsCommitIntervalMs());
        props.put(StreamsConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIG, INSTANCE.getStreamsBufferedRecordsPerPartition());

        // producer memory management
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, INSTANCE.getStreamsBufferMemory());
        props.put(ProducerConfig.SEND_BUFFER_CONFIG, INSTANCE.getStreamsSendBufferBytes());

        // consumer memory management
        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, INSTANCE.getStreamsFetchMaxBytes());
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, INSTANCE.getStreamsFetchMaxWaitMs());
        props.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, INSTANCE.getStreamsReceiveBufferBytes());


        // debug / metrics configs
        props.put(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, INSTANCE.metricsRecordingLevel);
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG), INSTANCE.consumerInterceptorClassesConfig);
        props.put(StreamsConfig.producerPrefix(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG), INSTANCE.producerInterceptorClassesConfig);


        return props;
    }

    private String parseEnv(String envVar, String defaultVal) {
        var value = Utils.coalesce(
                System.getProperty(envVar),
                System.getenv(envVar),
                dotenv.get(envVar),
                defaultVal
        );
        configMap.put(envVar, value);
        return value;
    }


}
