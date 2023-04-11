package org.geovistory.toolbox.streams.analysis.statements;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.analysis.statements.avro.AnalysisStatementKey;
import org.geovistory.toolbox.streams.analysis.statements.avro.AnalysisStatementValue;
import org.geovistory.toolbox.streams.avro.ProjectStatementKey;
import org.geovistory.toolbox.streams.avro.ProjectStatementValue;

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

    public Serde<AnalysisStatementKey> AnalysisStatementKey() {
        Serde<AnalysisStatementKey> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), true);
        return serdes;
    }

    public Serde<AnalysisStatementValue> AnalysisStatementValue() {
        Serde<AnalysisStatementValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
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


}
