package org.geovistory.toolbox.streams.lib;

import io.github.cdimascio.dotenv.Dotenv;


public enum AppConfig {
    INSTANCE();

    // url of the apicurio registry
    private String apicurioRegistryUrl;

    // kafka bootstrap servers (comma separated)
    private final String kafkaBootstrapServers;

    // application id
    private final String applicationId;

    // path of state store directory
    private final String stateDir;

    // prefix of output topics (generated by this app)
    private final String outputTopicPrefix;

    //prefix of input topics (generated by Debezium connector)
    private final String inputTopicPrefix;

    AppConfig() {
        // load .env for local development
        Dotenv dotenv = Dotenv
                .configure()
                .ignoreIfMissing()
                .load();


        this.apicurioRegistryUrl = Utils.coalesce(
                System.getProperty("TS_APICURIO_REGISTRY_URL"),
                System.getenv("TS_APICURIO_REGISTRY_URL"),
                dotenv.get("TS_APICURIO_REGISTRY_URL"),
                "http://localhost:8080/apis/registry/v2"
        );
        System.out.println("apicurioRegistryUrl: " + apicurioRegistryUrl);

        this.kafkaBootstrapServers = Utils.coalesce(
                System.getProperty("TS_BOOTSTRAP_SERVERS"),
                System.getenv("TS_BOOTSTRAP_SERVERS"),
                dotenv.get("TS_BOOTSTRAP_SERVERS"),
                "http://localhost:9092"
        );
        System.out.println("kafkaBootstrapServers: " + kafkaBootstrapServers);

        this.applicationId = Utils.coalesce(
                System.getProperty("TS_APPLICATION_ID"),
                System.getenv("TS_APPLICATION_ID"),
                dotenv.get("TS_APPLICATION_ID"),
                "dev"
        );
        System.out.println("applicationId: " + applicationId);

        this.stateDir = Utils.coalesce(
                System.getProperty("TS_STATE_DIR"),
                System.getenv("TS_STATE_DIR"),
                dotenv.get("TS_STATE_DIR"),
                "tmp/kafka-streams"
        );
        System.out.println("stateDir: " + stateDir);

        this.outputTopicPrefix = Utils.coalesce(
                System.getProperty("TS_OUTPUT_TOPIC_NAME_PREFIX"),
                System.getenv("TS_OUTPUT_TOPIC_NAME_PREFIX"),
                dotenv.get("TS_OUTPUT_TOPIC_NAME_PREFIX"),
                "dev"
        );
        System.out.println("topicPrefix: " + outputTopicPrefix);

        this.inputTopicPrefix = Utils.coalesce(
                System.getProperty("TS_INPUT_TOPIC_NAME_PREFIX"),
                System.getenv("TS_INPUT_TOPIC_NAME_PREFIX"),
                dotenv.get("TS_INPUT_TOPIC_NAME_PREFIX"),
                "dev"
        );
        System.out.println("topicPrefix: " + inputTopicPrefix);
    }

    public AppConfig getInstance() {
        return INSTANCE;
    }

    public String getStateDir() {
        return stateDir;
    }

    public String getApicurioRegistryUrl() {
        return apicurioRegistryUrl;
    }

    public void setApicurioRegistryUrl(String apicurioRegistryUrl) {
        this.apicurioRegistryUrl = apicurioRegistryUrl;
    }

    public String getKafkaBootstrapServers() {
        return kafkaBootstrapServers;
    }

    public String getApplicationId() {
        return applicationId;
    }

    public String getOutputTopicPrefix() {
        return outputTopicPrefix;
    }

    public String getInputTopicPrefix() {
        return inputTopicPrefix;
    }

}
