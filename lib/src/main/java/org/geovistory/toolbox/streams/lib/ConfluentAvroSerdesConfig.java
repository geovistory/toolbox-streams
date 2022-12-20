package org.geovistory.toolbox.streams.lib;

import java.util.HashMap;
import java.util.Map;

public enum ConfluentAvroSerdesConfig {
    INSTANCE();

    private final Map<String, Object> config;

    ConfluentAvroSerdesConfig() {
        this.config = new HashMap<>();
    }

    public ConfluentAvroSerdesConfig getInstance() {
        return INSTANCE;
    }

    public Map<String, Object> getConfig() {
        this.config.put("schema.registry.url", AppConfig.INSTANCE.getSchemaRegistryUrl());
        return config;
    }
}
