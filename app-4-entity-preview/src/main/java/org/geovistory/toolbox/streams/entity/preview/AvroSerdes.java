package org.geovistory.toolbox.streams.entity.preview;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.avro.*;

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

    public Serde<ProjectEntityLabelValue> ProjectEntityLabelValue() {
        Serde<ProjectEntityLabelValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<ProjectEntityFulltextValue> ProjectEntityFulltextValue() {
        Serde<ProjectEntityFulltextValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }


    public Serde<TimeSpanValue> TimeSpanValue() {
        Serde<TimeSpanValue> serdes = new SpecificAvroSerde<>();
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

    public Serde<EntityPreviewValue> EntityPreviewValue() {
        Serde<EntityPreviewValue> serdes = new SpecificAvroSerde<>();
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

    public Serde<CommunityEntityLabelValue> CommunityEntityLabelValue() {
        Serde<CommunityEntityLabelValue> serdes = new SpecificAvroSerde<>();
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


    public Serde<CommunityEntityFulltextValue> CommunityEntityFulltextValue() {
        Serde<CommunityEntityFulltextValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }


    public Serde<CommunityEntityTypeValue> CommunityEntityTypeValue() {
        Serde<CommunityEntityTypeValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

}
