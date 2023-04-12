package org.geovistory.toolbox.streams.fulltext;

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


    public Serde<ProjectFieldLabelKey> ProjectPropertyLabelKey() {
        Serde<ProjectFieldLabelKey> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), true);
        return serdes;
    }

    public Serde<ProjectFieldLabelValue> ProjectPropertyLabelValue() {
        Serde<ProjectFieldLabelValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }


    public Serde<ProjectEntityKey> ProjectEntityKey() {
        Serde<ProjectEntityKey> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), true);
        return serdes;
    }


    public Serde<CommunityEntityLabelConfigValue> CommunityEntityLabelConfigValue() {
        Serde<CommunityEntityLabelConfigValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<ProjectEntityLabelConfigValue> ProjectEntityLabelConfigValue() {
        Serde<ProjectEntityLabelConfigValue> serdes = new SpecificAvroSerde<>();
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

    public Serde<ProjectEntityFulltextValue> ProjectEntityFulltextValue() {
        Serde<ProjectEntityFulltextValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }


    public Serde<CommunityPropertyLabelKey> CommunityPropertyLabelKey() {
        Serde<CommunityPropertyLabelKey> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), true);
        return serdes;
    }

    public Serde<CommunityPropertyLabelValue> CommunityPropertyLabelValue() {
        Serde<CommunityPropertyLabelValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<CommunityEntityKey> CommunityEntityKey() {
        Serde<CommunityEntityKey> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), true);
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

    public Serde<CommunityEntityFulltextValue> CommunityEntityFulltextValue() {
        Serde<CommunityEntityFulltextValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<FieldLabelWithTopLabelsValue> FieldLabelWithTopLabelsValue() {
        Serde<FieldLabelWithTopLabelsValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<ProjectFieldTopLabelsValue> ProjectFieldTopLabelsValue() {
        Serde<ProjectFieldTopLabelsValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<EntityFieldTextMapValue> EntityFieldTextMapValue() {
        Serde<EntityFieldTextMapValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<EntityFieldTextMapWithConfigValue> EntityFieldTextMapWithConfigValue() {
        Serde<EntityFieldTextMapWithConfigValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<CommunityFieldTopLabelsValue> CommunityFieldTopLabelsValue() {
        Serde<CommunityFieldTopLabelsValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }
}
