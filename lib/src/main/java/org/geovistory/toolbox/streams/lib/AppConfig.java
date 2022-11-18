package org.geovistory.toolbox.streams.lib;

import io.github.cdimascio.dotenv.Dotenv;


public enum AppConfig {
    INSTANCE();

    // url of the apicurio registry
    private String apicurioRegistryUrl;

    // kafka bootstrap servers (comma separated)
    private String kafkaBootstrapServers;

    // application id
    private String applicationId;

    // path of state store directory
    private String stateDir;

    // prefix of topics
    private String topicPrefix;

    AppConfig() {
        // load .env for local development
        Dotenv dotenv = Dotenv
                .configure()
                .ignoreIfMissing()
                .load();


        this.apicurioRegistryUrl = Utils.coalesce(
                System.getProperty("KS_APICURIO_REGISTRY_URL"),
                System.getenv("KS_APICURIO_REGISTRY_URL"),
                dotenv.get("KS_APICURIO_REGISTRY_URL"),
                "http://localhost:8080/apis/registry/v2"
        );
        System.out.println("apicurioRegistryUrl: " + apicurioRegistryUrl);

        this.kafkaBootstrapServers = Utils.coalesce(
                System.getProperty("KS_BOOTSTRAP_SERVERS"),
                System.getenv("KS_BOOTSTRAP_SERVERS"),
                dotenv.get("KS_BOOTSTRAP_SERVERS"),
                "http://localhost:9092"
        );
        System.out.println("kafkaBootstrapServers: " + kafkaBootstrapServers);

        this.applicationId = Utils.coalesce(
                System.getProperty("KS_APPLICATION_ID"),
                System.getenv("KS_APPLICATION_ID"),
                dotenv.get("KS_APPLICATION_ID"),
                "dev"
        );
        System.out.println("applicationId: " + applicationId);

        this.stateDir = Utils.coalesce(
                System.getProperty("KS_STATE_DIR"),
                System.getenv("KS_STATE_DIR"),
                dotenv.get("KS_STATE_DIR"),
                "tmp/kafka-streams"
        );
        System.out.println("stateDir: " + stateDir);

        this.topicPrefix = Utils.coalesce(
                System.getProperty("KS_TOPIC_NAME_PREFIX"),
                System.getenv("KS_TOPIC_NAME_PREFIX"),
                dotenv.get("KS_TOPIC_NAME_PREFIX"),
                "dev"
        );
        System.out.println("topicPrefix: " + topicPrefix);
    }

    public AppConfig getInstance() {
        return INSTANCE;
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

    public void setKafkaBootstrapServers(String kafkaBootstrapServers) {
        this.kafkaBootstrapServers = kafkaBootstrapServers;
    }

    public String getApplicationId() {
        return applicationId;
    }

    public void setApplicationId(String applicationId) {
        this.applicationId = applicationId;
    }

    public String getStateDir() {
        return stateDir;
    }

    public void setStateDir(String stateDir) {
        this.stateDir = stateDir;
    }

    public String getTopicPrefix() {
        return topicPrefix;
    }

    public void setTopicPrefix(String topicPrefix) {
        this.topicPrefix = topicPrefix;
    }
}
