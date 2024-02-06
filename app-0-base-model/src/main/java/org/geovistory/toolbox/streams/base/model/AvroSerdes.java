package org.geovistory.toolbox.streams.base.model;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.avro.*;

import jakarta.enterprise.context.ApplicationScoped;
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


    public Serde<dev.data_for_history.api_property.Key> DfhApiPropertyKey() {
        Serde<dev.data_for_history.api_property.Key> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), true);
        return serdes;
    }

    public Serde<dev.data_for_history.api_property.Value> DfhApiPropertyValue() {
        Serde<dev.data_for_history.api_property.Value> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<dev.data_for_history.api_class.Key> DfhApiClassKey() {
        Serde<dev.data_for_history.api_class.Key> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), true);
        return serdes;
    }

    public Serde<dev.data_for_history.api_class.Value> DfhApiClassValue() {
        Serde<dev.data_for_history.api_class.Value> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<OntomePropertyKey> OntomePropertyKey() {
        Serde<OntomePropertyKey> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), true);
        return serdes;
    }

    public Serde<OntomePropertyValue> OntomePropertyValue() {
        Serde<OntomePropertyValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<OntomePropertyLabelKey> OntomePropertyLabelKey() {
        Serde<OntomePropertyLabelKey> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), true);
        return serdes;
    }

    public Serde<OntomePropertyLabelValue> OntomePropertyLabelValue() {
        Serde<OntomePropertyLabelValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<OntomeClassKey> OntomeClassKey() {
        Serde<OntomeClassKey> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), true);
        return serdes;
    }

    public Serde<OntomeClassValue> OntomeClassValue() {
        Serde<OntomeClassValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<OntomeClassLabelKey> OntomeClassLabelKey() {
        Serde<OntomeClassLabelKey> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), true);
        return serdes;
    }

    public Serde<OntomeClassLabelValue> OntomeClassLabelValue() {
        Serde<OntomeClassLabelValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<OntomeClassMetadataValue> OntomeClassMetadataValue() {
        Serde<OntomeClassMetadataValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<HasTypePropertyKey> HasTypePropertyKey() {
        Serde<HasTypePropertyKey> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), true);
        return serdes;
    }

    public Serde<HasTypePropertyGroupByValue> HasTypePropertyGroupByValue() {
        Serde<HasTypePropertyGroupByValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }


    public Serde<HasTypePropertyAggregateValue> HasTypePropertyAggregateValue() {
        Serde<HasTypePropertyAggregateValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<HasTypePropertyValue> HasTypePropertyValue() {
        Serde<HasTypePropertyValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

}
