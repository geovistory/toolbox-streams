package org.geovistory.toolbox.streams.entity.label;

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
    public Serde<ts.projects.info_proj_rel.Key> ProInfoProjRelKey() {
        Serde<ts.projects.info_proj_rel.Key> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), true);
        return serdes;
    }

    public Serde<ts.projects.info_proj_rel.Value> ProInfoProjRelValue() {
        Serde<ts.projects.info_proj_rel.Value> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
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


    public Serde<BooleanMap> BooleanMapValue() {
        Serde<BooleanMap> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<ProjectClassKey> ProjectClassKey() {
        Serde<ProjectClassKey> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), true);

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

    public Serde<ts.information.statement.Key> InfStatementKey() {
        Serde<ts.information.statement.Key> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), true);
        return serdes;
    }


    public Serde<StatementEnrichedValue> StatementEnrichedValue() {
        Serde<StatementEnrichedValue> serdes = new SpecificAvroSerde<>();
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
    public Serde<ProjectEdgeValue> ProjectEdgeValue() {
        Serde<ProjectEdgeValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }
    public Serde<CommunityEntityLabelConfigKey> CommunityEntityLabelConfigKey() {
        Serde<CommunityEntityLabelConfigKey> serdes = new SpecificAvroSerde<>();
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

    public Serde<ProjectEntityWithConfigValue> ProjectEntityWithConfigValue() {
        Serde<ProjectEntityWithConfigValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<ProjectEntityLabelPartKey> ProjectEntityLabelPartKey() {
        Serde<ProjectEntityLabelPartKey> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), true);
        return serdes;
    }

    public Serde<ProjectEntityLabelPartValue> ProjectEntityLabelPartValue() {
        Serde<ProjectEntityLabelPartValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<EntityLabelSlotWithStringValue> ProjectEntityLabelSlotWithStringValue() {
        Serde<EntityLabelSlotWithStringValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<ProjectEntityLabelValue> ProjectEntityLabelValue() {
        Serde<ProjectEntityLabelValue> serdes = new SpecificAvroSerde<>();
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

    public Serde<ProjectEntityVisibilityValue> ProjectEntityVisibilityValue() {
        Serde<ProjectEntityVisibilityValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<CommunityStatementKey> CommunityStatementKey() {
        Serde<CommunityStatementKey> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), true);
        return serdes;
    }

    public Serde<CommunityStatementValue> CommunityStatementValue() {
        Serde<CommunityStatementValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<DRMap> DRMap() {
        Serde<DRMap> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<CommunityEntityLabelValue> CommunityEntityLabelValue() {
        Serde<CommunityEntityLabelValue> serdes = new SpecificAvroSerde<>();
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

    public Serde<CommunityEntityWithConfigValue> CommunityEntityWithConfigValue() {
        Serde<CommunityEntityWithConfigValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<CommunityEntityLabelPartKey> CommunityEntityLabelPartKey() {
        Serde<CommunityEntityLabelPartKey> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), true);
        return serdes;
    }

    public Serde<CommunityEntityLabelPartValue> CommunityEntityLabelPartValue() {
        Serde<CommunityEntityLabelPartValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

}
