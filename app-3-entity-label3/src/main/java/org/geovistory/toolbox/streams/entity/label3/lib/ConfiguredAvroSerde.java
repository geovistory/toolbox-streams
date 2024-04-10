package org.geovistory.toolbox.streams.entity.label3.lib;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
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

    public <V extends SpecificRecord> Serializer<V> vS() {
        try (var v = this.<V>value()) {
            return v.serializer();
        }
    }

    public <K extends SpecificRecord> Serializer<K> kS() {
        try (var v = this.<K>key()) {
            return v.serializer();
        }
    }

    public <V extends SpecificRecord> Deserializer<V> vD() {
        try (var v = this.<V>value()) {
            return v.deserializer();
        }
    }

    public <K extends SpecificRecord> Deserializer<K> kD() {
        try (var v = this.<K>key()) {
            return v.deserializer();
        }
    }


    private SpecificAvroSerde<SpecificRecord> createConfiguredAvroSerde(Boolean isKey) {
        var properties = new HashMap<String, String>();
        properties.put("schema.registry.url", schemaRegistryUrl);
        var s = new SpecificAvroSerde<>();
        s.configure(properties, isKey);
        return s;
    }

}