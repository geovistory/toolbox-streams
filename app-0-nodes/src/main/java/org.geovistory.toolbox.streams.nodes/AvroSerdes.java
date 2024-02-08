package org.geovistory.toolbox.streams.nodes;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.avro.NodeKey;
import org.geovistory.toolbox.streams.avro.NodeValue;
import org.geovistory.toolbox.streams.avro.TextValue;

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


    public Serde<ts.information.resource.Key> InfResourceKey() {
        Serde<ts.information.resource.Key> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), true);
        return serdes;
    }

    public Serde<ts.information.resource.Value> InfResourceValue() {
        Serde<ts.information.resource.Value> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }


    public Serde<ts.information.language.Key> InfLanguageKey() {
        Serde<ts.information.language.Key> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), true);
        return serdes;
    }


    public Serde<ts.information.language.Value> InfLanguageValue() {
        Serde<ts.information.language.Value> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<ts.information.appellation.Key> InfAppellationKey() {
        Serde<ts.information.appellation.Key> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), true);
        return serdes;
    }

    public Serde<ts.information.appellation.Value> InfAppellationValue() {
        Serde<ts.information.appellation.Value> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<ts.information.lang_string.Key> InfLangStringKey() {
        Serde<ts.information.lang_string.Key> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), true);
        return serdes;
    }

    public Serde<ts.information.lang_string.Value> InfLangStringValue() {
        Serde<ts.information.lang_string.Value> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<ts.information.place.Key> InfPlaceKey() {
        Serde<ts.information.place.Key> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), true);
        return serdes;
    }

    public Serde<ts.information.place.Value> InfPlaceValue() {
        Serde<ts.information.place.Value> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<ts.information.time_primitive.Key> InfTimePrimitiveKey() {
        Serde<ts.information.time_primitive.Key> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), true);
        return serdes;
    }

    public Serde<ts.information.time_primitive.Value> InfTimePrimitiveValue() {
        Serde<ts.information.time_primitive.Value> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<ts.information.dimension.Key> InfDimensionKey() {
        Serde<ts.information.dimension.Key> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), true);
        return serdes;
    }

    public Serde<ts.information.dimension.Value> InfDimensionValue() {
        Serde<ts.information.dimension.Value> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<ts.information.statement.Key> InfStatementKey() {
        Serde<ts.information.statement.Key> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), true);
        return serdes;
    }

    public Serde<ts.information.statement.Value> InfStatementValue() {
        Serde<ts.information.statement.Value> serdes = new SpecificAvroSerde<>();
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

    public Serde<ts.data.digital.Key> DatDigitalKey() {
        Serde<ts.data.digital.Key> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), true);
        return serdes;
    }

    public Serde<ts.data.digital.Value> DatDigitalValue() {
        Serde<ts.data.digital.Value> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<ts.tables.cell.Key> TabCellKey() {
        Serde<ts.tables.cell.Key> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), true);
        return serdes;
    }

    public Serde<ts.tables.cell.Value> TabCellValue() {
        Serde<ts.tables.cell.Value> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<TextValue> TextValue() {
        Serde<TextValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }
}
