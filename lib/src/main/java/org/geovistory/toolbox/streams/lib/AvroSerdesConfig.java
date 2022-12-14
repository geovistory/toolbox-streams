package org.geovistory.toolbox.streams.lib;

import io.apicurio.registry.serde.SerdeConfig;
import io.apicurio.registry.serde.avro.AvroKafkaSerdeConfig;

import java.util.HashMap;
import java.util.Map;

public enum AvroSerdesConfig {
    INSTANCE();

    private final Map<String, Object> config;

    AvroSerdesConfig() {

        // configure org.geovistory.toolbox.streams.lib.AvroSerdes config
        this.config = new HashMap<>();
        this.config.put(SerdeConfig.AUTO_REGISTER_ARTIFACT, true);
        this.config.put(AvroKafkaSerdeConfig.USE_SPECIFIC_AVRO_READER, true);

    }

    public AvroSerdesConfig getInstance() {
        return INSTANCE;
    }

    public Map<String, Object> getConfig() {
        this.config.put(SerdeConfig.REGISTRY_URL, AppConfig.INSTANCE.getApicurioRegistryUrl());
        return config;
    }
}
