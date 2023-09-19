package org.geovistory.toolbox.streams.entity;

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

    public Serde<OntomeClassLabelKey> OntomeClassLabelKey() {
        Serde<OntomeClassLabelKey> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), true);
        return serdes;
    }

    public Serde<CommunityClassLabelValue> CommunityClassLabelValue() {
        Serde<CommunityClassLabelValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }


    public Serde<ProjectClassLabelKey> ProjectClassLabelKey() {
        Serde<ProjectClassLabelKey> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), true);
        return serdes;
    }

    public Serde<ProjectClassLabelValue> ProjectClassLabelValue() {
        Serde<ProjectClassLabelValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<ProjectEntityKey> ProjectEntityKey() {
        Serde<ProjectEntityKey> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), true);
        return serdes;
    }

    public Serde<ProjectEntityValue> ProjectEntityValue() {
        Serde<ProjectEntityValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<ProjectTopStatementsKey> ProjectTopStatementsKey() {
        Serde<ProjectTopStatementsKey> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), true);
        return serdes;
    }

    public Serde<ProjectTopStatementsValue> ProjectTopStatementsValue() {
        Serde<ProjectTopStatementsValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<TimeSpanValue> TimeSpanValue() {
        Serde<TimeSpanValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<HasTypePropertyKey> HasTypePropertyKey() {
        Serde<HasTypePropertyKey> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), true);
        return serdes;
    }

    public Serde<HasTypePropertyValue> HasTypePropertyValue() {
        Serde<HasTypePropertyValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<ProjectEntityHasTypePropValue> ProjectEntityHasTypePropValue() {
        Serde<ProjectEntityHasTypePropValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<ProjectEntityTypeValue> ProjectEntityTypeValue() {
        Serde<ProjectEntityTypeValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }


    public Serde<ProjectEntityClassLabelValue> ProjectEntityClassLabelValue() {
        Serde<ProjectEntityClassLabelValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }


    public Serde<OntomeClassKey> OntomeClassKey() {
        Serde<OntomeClassKey> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), true);
        return serdes;
    }


    public Serde<OntomeClassMetadataValue> OntomeClassMetadataValue() {
        Serde<OntomeClassMetadataValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<ProjectEntityClassMetadataValue> ProjectEntityClassMetadataValue() {
        Serde<ProjectEntityClassMetadataValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }


    public Serde<CommunityEntityKey> CommunityEntityKey() {
        Serde<CommunityEntityKey> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), true);
        return serdes;
    }


    public Serde<CommunityEntityValue> CommunityEntityValue() {
        Serde<CommunityEntityValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<CommunityTopStatementsKey> CommunityTopStatementsKey() {
        Serde<CommunityTopStatementsKey> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), true);
        return serdes;
    }

    public Serde<CommunityTopStatementsValue> CommunityTopStatementsValue() {
        Serde<CommunityTopStatementsValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<CommunityEntityClassLabelValue> CommunityEntityClassLabelValue() {
        Serde<CommunityEntityClassLabelValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<CommunityEntityClassMetadataValue> CommunityEntityClassMetadataValue() {
        Serde<CommunityEntityClassMetadataValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }


    public Serde<CommunityEntityTypeValue> CommunityEntityTypeValue() {
        Serde<CommunityEntityTypeValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<CommunityEntityHasTypePropValue> CommunityEntityHasTypePropValue() {
        Serde<CommunityEntityHasTypePropValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<TopTimePrimitives> TopTimePrimitives() {
        Serde<TopTimePrimitives> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<TopTimePrimitivesMap> TopTimePrimitivesMap() {
        Serde<TopTimePrimitivesMap> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

}
