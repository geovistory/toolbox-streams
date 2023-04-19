package org.geovistory.toolbox.streams.nodes;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.avro.NodeKey;
import org.geovistory.toolbox.streams.avro.NodeValue;
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


    public Serde<dev.information.resource.Key> InfResourceKey() {
        Serde<dev.information.resource.Key> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), true);
        return serdes;
    }

    public Serde<dev.information.resource.Value> InfResourceValue() {
        Serde<dev.information.resource.Value> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }


    public Serde<dev.information.language.Key> InfLanguageKey() {
        Serde<dev.information.language.Key> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), true);
        return serdes;
    }


    public Serde<dev.information.language.Value> InfLanguageValue() {
        Serde<dev.information.language.Value> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<dev.information.appellation.Key> InfAppellationKey() {
        Serde<dev.information.appellation.Key> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), true);
        return serdes;
    }

    public Serde<dev.information.appellation.Value> InfAppellationValue() {
        Serde<dev.information.appellation.Value> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<dev.information.lang_string.Key> InfLangStringKey() {
        Serde<dev.information.lang_string.Key> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), true);
        return serdes;
    }

    public Serde<dev.information.lang_string.Value> InfLangStringValue() {
        Serde<dev.information.lang_string.Value> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<dev.information.place.Key> InfPlaceKey() {
        Serde<dev.information.place.Key> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), true);
        return serdes;
    }

    public Serde<dev.information.place.Value> InfPlaceValue() {
        Serde<dev.information.place.Value> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<dev.information.time_primitive.Key> InfTimePrimitiveKey() {
        Serde<dev.information.time_primitive.Key> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), true);
        return serdes;
    }

    public Serde<dev.information.time_primitive.Value> InfTimePrimitiveValue() {
        Serde<dev.information.time_primitive.Value> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<dev.information.dimension.Key> InfDimensionKey() {
        Serde<dev.information.dimension.Key> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), true);
        return serdes;
    }

    public Serde<dev.information.dimension.Value> InfDimensionValue() {
        Serde<dev.information.dimension.Value> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
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

    public Serde<dev.data.digital.Key> DatDigitalKey() {
        Serde<dev.data.digital.Key> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), true);
        return serdes;
    }

    public Serde<dev.data.digital.Value> DatDigitalValue() {
        Serde<dev.data.digital.Value> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<dev.tables.cell.Key> TabCellKey() {
        Serde<dev.tables.cell.Key> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), true);
        return serdes;
    }

    public Serde<dev.tables.cell.Value> TabCellValue() {
        Serde<dev.tables.cell.Value> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<TextValue> TextValue() {
        Serde<TextValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }
}
