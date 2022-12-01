package org.geovistory.toolbox.streams.utilities.config;

import io.github.cdimascio.dotenv.Dotenv;
import org.geovistory.toolbox.streams.lib.Utils;


public enum AppEnvConfig {
    INSTANCE();

    // url of the apicurio registry
    private final String apicurioRegistryUrl;

    // kafka bootstrap servers (comma separated)
    private final String kafkaBootstrapServers;

    AppEnvConfig() {
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


    }

    public String getApicurioRegistryUrl() {
        return apicurioRegistryUrl;
    }

    public String getKafkaBootstrapServers() {
        return kafkaBootstrapServers;
    }

}
