package org.geovistory.toolbox.streams.project.items.lib;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.HashMap;

@ApplicationScoped
public class ConfiguredAvroSerde {
    @ConfigProperty(name = "schema.registry.url")
    public String schemaRegistryUrl;
    private Serde configuredKeySerde;
    private Serde configuredValueSerde;

    public <T extends SpecificRecord> Serde<T> key() {
        if (configuredKeySerde != null) return configuredKeySerde;
        configuredKeySerde = createConfiguredAvroSerde(true);
        return configuredKeySerde;
    }

    public <T extends SpecificRecord> Serde<T> value() {
        if (configuredValueSerde != null) return configuredValueSerde;
        configuredValueSerde = createConfiguredAvroSerde(false);
        return configuredValueSerde;
    }

    private SpecificAvroSerde<SpecificRecord> createConfiguredAvroSerde(Boolean isKey) {
        var properties = new HashMap<String, String>();
        properties.put("schema.registry.url", schemaRegistryUrl);
        var s = new SpecificAvroSerde<>();
        s.configure(properties, isKey);
        return s;
    }

}