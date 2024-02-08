package org.geovistory.toolbox.streams.base.config;

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

    public Serde<ts.system.config.Key> SysConfigKey() {
        Serde<ts.system.config.Key> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), true);
        return serdes;
    }

    public Serde<ts.system.config.Value> SysConfigValue() {
        Serde<ts.system.config.Value> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<ts.projects.dfh_profile_proj_rel.Key> ProProfileProjRelKey() {
        Serde<ts.projects.dfh_profile_proj_rel.Key> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), true);
        return serdes;
    }

    public Serde<ts.projects.dfh_profile_proj_rel.Value> ProProfileProjRelValue() {
        Serde<ts.projects.dfh_profile_proj_rel.Value> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<ProjectProfileKey> ProjectProfileKey() {
        Serde<ProjectProfileKey> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), true);
        return serdes;
    }

    public Serde<BooleanMap> BooleanMapValue() {
        Serde<BooleanMap> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<ProjectProfileValue> ProjectProfileValue() {
        Serde<ProjectProfileValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<ProjectPropertyKey> ProjectPropertyKey() {
        Serde<ProjectPropertyKey> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), true);

        return serdes;
    }

    public Serde<ProjectPropertyValue> ProjectPropertyValue() {
        Serde<ProjectPropertyValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);

        return serdes;
    }


    public Serde<ProfilePropertyMap> ProfilePropertyMapValue() {
        Serde<ProfilePropertyMap> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<ProjectClassKey> ProjectClassKey() {
        Serde<ProjectClassKey> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), true);

        return serdes;
    }

    public Serde<ProjectClassValue> ProjectClassValue() {
        Serde<ProjectClassValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);

        return serdes;
    }

    public Serde<ProfileClass> ProfileClassValue() {
        Serde<ProfileClass> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<ProfileClassMap> ProfileClassMapValue() {
        Serde<ProfileClassMap> serdes = new SpecificAvroSerde<>();
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

    public Serde<GeovClassLabelKey> GeovClassLabelKey() {
        Serde<GeovClassLabelKey> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), true);
        return serdes;
    }

    public Serde<GeovClassLabelValue> GeovClassLabelValue() {
        Serde<GeovClassLabelValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<CommunityClassLabelValue> CommunityClassLabelValue() {
        Serde<CommunityClassLabelValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<GeovPropertyLabelKey> GeovPropertyLabelKey() {
        Serde<GeovPropertyLabelKey> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), true);
        return serdes;
    }

    public Serde<GeovPropertyLabelValue> GeovPropertyLabelValue() {
        Serde<GeovPropertyLabelValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<ts.projects.text_property.Key> ProTextPropertyKey() {
        Serde<ts.projects.text_property.Key> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), true);
        return serdes;
    }

    public Serde<ts.projects.text_property.Value> ProTextPropertyValue() {
        Serde<ts.projects.text_property.Value> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<ProjectClassLanguageKey> ProjectClassLanguageKey() {
        Serde<ProjectClassLanguageKey> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), true);
        return serdes;
    }

    public Serde<ProjectClassLanguageValue> ProjectClassLanguageValue() {
        Serde<ProjectClassLanguageValue> serdes = new SpecificAvroSerde<>();
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

    public Serde<ProjectClassLabelOptionMap> ProjectClassLabelOptionMapValue() {
        Serde<ProjectClassLabelOptionMap> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }


    public Serde<ProjectFieldLanguageKey> ProjectPropertyLanguageKey() {
        Serde<ProjectFieldLanguageKey> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), true);
        return serdes;
    }

    public Serde<ProjectFieldLanguageValue> ProjectFieldLanguageValue() {
        Serde<ProjectFieldLanguageValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }

    public Serde<ProjectPropertyLanguageValue> ProjectPropertyLanguageValue() {
        Serde<ProjectPropertyLanguageValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
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

    public Serde<ProjectFieldLabelOptionMap> ProjectPropertyLabelOptionMapValue() {
        Serde<ProjectFieldLabelOptionMap> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }


    public Serde<ts.projects.entity_label_config.Key> ProEntityLabelConfigKey() {
        Serde<ts.projects.entity_label_config.Key> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), true);
        return serdes;
    }

    public Serde<ts.projects.entity_label_config.Value> ProEntityLabelConfigValue() {
        Serde<ts.projects.entity_label_config.Value> serdes = new SpecificAvroSerde<>();
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

    public Serde<IntegerList> IntegerList() {
        Serde<IntegerList> serdes = new SpecificAvroSerde<>();
        serdes.configure(getProperties(), false);
        return serdes;
    }


}
