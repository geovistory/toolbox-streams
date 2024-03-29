package org.geovistory.toolbox.streams.rdf;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.kafka.common.serialization.Serde;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.avro.*;

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

    public Serde<ProjectStatementKey> ProjectStatementKey() {
        Serde<ProjectStatementKey> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), true);
        return serdes;
    }

    public Serde<ProjectStatementValue> ProjectStatementValue() {
        Serde<ProjectStatementValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<ProjectRdfKey> ProjectRdfKey() {
        Serde<ProjectRdfKey> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), true);
        return serdes;
    }

    public Serde<ProjectRdfValue> ProjectRdfValue() {
        Serde<ProjectRdfValue> serdes = new SpecificAvroSerde<>();
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

    public Serde<ProjectEntityLabelValue> ProjectEntityLabelValue() {
        Serde<ProjectEntityLabelValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<ProjectRdfList> ProjectRdfList() {
        Serde<ProjectRdfList> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<OntomePropertyLabelKey> OntomePropertyLabelKey() {
        Serde<OntomePropertyLabelKey> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), true);
        return serdes;
    }

    public Serde<TextWithDeleteValue> TextWithDeleteValue() {
        Serde<TextWithDeleteValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<OntomePropertyLabelValue> OntomePropertyLabelValue() {
        Serde<OntomePropertyLabelValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<ProjectOwlPropertyKey> ProjectOwlPropertyKey() {
        Serde<ProjectOwlPropertyKey> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), true);
        return serdes;
    }

    public Serde<ProjectOwlPropertyKey> ProjectOwlPropertyKeyAsValue() {
        Serde<ProjectOwlPropertyKey> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<ProjectOwlPropertyValue> ProjectOwlPropertyValue() {
        Serde<ProjectOwlPropertyValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<ProjectOwlPropertyLabelValue> ProjectOwlPropertyLabelValue() {
        Serde<ProjectOwlPropertyLabelValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<ts.projects.project.Key> ProProjectKey() {
        Serde<ts.projects.project.Key> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), true);
        return serdes;
    }

    public Serde<ts.projects.project.Value> ProProjectValue() {
        Serde<ts.projects.project.Value> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }
}
