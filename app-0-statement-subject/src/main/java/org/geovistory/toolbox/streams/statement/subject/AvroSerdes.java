package org.geovistory.toolbox.streams.statement.subject;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.avro.NodeKey;
import org.geovistory.toolbox.streams.avro.NodeValue;
import org.geovistory.toolbox.streams.avro.StatementEnrichedValue;
import org.geovistory.toolbox.streams.avro.TextValue;

import javax.enterprise.context.ApplicationScoped;
import java.util.HashMap;
import java.util.Map;


@ApplicationScoped
public class AvroSerdes {
    @ConfigProperty(name = "quarkus.kafka.streams.schema.registry.url")
    public String QUARKUS_KAFKA_STREAMS_SCHEMA_REGISTRY_URL;

    private final Map<String, Object> properties = new HashMap<>();

    public Map<String, Object> getProperties() {
        this.properties.put("schema.registry.url", QUARKUS_KAFKA_STREAMS_SCHEMA_REGISTRY_URL);
        return properties;
    }


    public Serde<dev.information.statement.Key> InfStatementKey() {
        Serde<dev.information.statement.Key> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), true);
        return serdes;
    }

    public Serde<dev.information.statement.Value> InfStatementValue() {
        Serde<dev.information.statement.Value> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<NodeKey> NodeKey() {
        Serde<NodeKey> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), true);
        return serdes;
    }

    public Serde<NodeValue> NodeValue() {
        Serde<NodeValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }


    public Serde<StatementEnrichedValue> StatementEnrichedValue() {
        Serde<StatementEnrichedValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<TextValue> TextValue() {
        Serde<TextValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

}
