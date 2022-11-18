package org.geovistory.toolbox.streams.utilities.config;

import io.github.cdimascio.dotenv.Dotenv;
import org.geovistory.toolbox.streams.lib.Utils;


public enum AppEnvConfig {
    INSTANCE();

    // url of the apicurio registry
    private String apicurioRegistryUrl;

    // kafka bootstrap servers (comma separated)
    private String kafkaBootstrapServers;

    AppEnvConfig() {
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


    }

    public AppEnvConfig getInstance() {
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
}
