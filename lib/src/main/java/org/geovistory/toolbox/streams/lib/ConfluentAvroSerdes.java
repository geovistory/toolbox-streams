package org.geovistory.toolbox.streams.lib;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.geovistory.toolbox.streams.avro.*;


public class ConfluentAvroSerdes {

    public Serde<ts.projects.dfh_profile_proj_rel.Key> ProProfileProjRelKey() {
        Serde<ts.projects.dfh_profile_proj_rel.Key> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }

    public Serde<ts.projects.dfh_profile_proj_rel.Value> ProProfileProjRelValue() {
        Serde<ts.projects.dfh_profile_proj_rel.Value> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<ts.projects.info_proj_rel.Key> ProInfoProjRelKey() {
        Serde<ts.projects.info_proj_rel.Key> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }

    public Serde<ts.projects.info_proj_rel.Value> ProInfoProjRelValue() {
        Serde<ts.projects.info_proj_rel.Value> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<ts.information.resource.Key> InfResourceKey() {
        Serde<ts.information.resource.Key> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }

    public Serde<ts.information.resource.Value> InfResourceValue() {
        Serde<ts.information.resource.Value> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }


    public Serde<ts.data_for_history.api_property.Key> DfhApiPropertyKey() {
        Serde<ts.data_for_history.api_property.Key> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }

    public Serde<ts.data_for_history.api_property.Value> DfhApiPropertyValue() {
        Serde<ts.data_for_history.api_property.Value> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<ts.data_for_history.api_class.Key> DfhApiClassKey() {
        Serde<ts.data_for_history.api_class.Key> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }

    public Serde<ts.data_for_history.api_class.Value> DfhApiClassValue() {
        Serde<ts.data_for_history.api_class.Value> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<ts.projects.project.Key> ProProjectKey() {
        Serde<ts.projects.project.Key> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }

    public Serde<ts.projects.project.Value> ProProjectValue() {
        Serde<ts.projects.project.Value> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<ts.system.config.Key> SysConfigKey() {
        Serde<ts.system.config.Key> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }

    public Serde<ts.system.config.Value> SysConfigValue() {
        Serde<ts.system.config.Value> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }


    public Serde<ProjectProfileKey> ProjectProfileKey() {
        Serde<ProjectProfileKey> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }

    public Serde<BooleanMap> BooleanMapValue() {
        Serde<BooleanMap> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<ProjectProfileValue> ProjectProfileValue() {
        Serde<ProjectProfileValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<ProjectPropertyKey> ProjectPropertyKey() {
        Serde<ProjectPropertyKey> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), true);

        return serdes;
    }

    public Serde<ProjectPropertyValue> ProjectPropertyValue() {
        Serde<ProjectPropertyValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);

        return serdes;
    }


    public Serde<ProfilePropertyMap> ProfilePropertyMapValue() {
        Serde<ProfilePropertyMap> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<ProjectClassKey> ProjectClassKey() {
        Serde<ProjectClassKey> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), true);

        return serdes;
    }

    public Serde<ProjectClassValue> ProjectClassValue() {
        Serde<ProjectClassValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);

        return serdes;
    }

    public Serde<ProfileClass> ProfileClassValue() {
        Serde<ProfileClass> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<ProfileClassMap> ProfileClassMapValue() {
        Serde<ProfileClassMap> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<OntomeClassLabelKey> OntomeClassLabelKey() {
        Serde<OntomeClassLabelKey> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }

    public Serde<OntomeClassLabelValue> OntomeClassLabelValue() {
        Serde<OntomeClassLabelValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<OntomePropertyLabelKey> OntomePropertyLabelKey() {
        Serde<OntomePropertyLabelKey> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }

    public Serde<OntomePropertyLabelValue> OntomePropertyLabelValue() {
        Serde<OntomePropertyLabelValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<GeovClassLabelKey> GeovClassLabelKey() {
        Serde<GeovClassLabelKey> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }

    public Serde<GeovClassLabelValue> GeovClassLabelValue() {
        Serde<GeovClassLabelValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<CommunityClassLabelValue> CommunityClassLabelValue() {
        Serde<CommunityClassLabelValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<GeovPropertyLabelKey> GeovPropertyLabelKey() {
        Serde<GeovPropertyLabelKey> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }

    public Serde<GeovPropertyLabelValue> GeovPropertyLabelValue() {
        Serde<GeovPropertyLabelValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<ts.projects.text_property.Key> ProTextPropertyKey() {
        Serde<ts.projects.text_property.Key> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }

    public Serde<ts.projects.text_property.Value> ProTextPropertyValue() {
        Serde<ts.projects.text_property.Value> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<ProjectClassLanguageKey> ProjectClassLanguageKey() {
        Serde<ProjectClassLanguageKey> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }

    public Serde<ProjectClassLanguageValue> ProjectClassLanguageValue() {
        Serde<ProjectClassLanguageValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }


    public Serde<ProjectClassLabelKey> ProjectClassLabelKey() {
        Serde<ProjectClassLabelKey> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }

    public Serde<ProjectClassLabelValue> ProjectClassLabelValue() {
        Serde<ProjectClassLabelValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<ProjectClassLabelOptionMap> ProjectClassLabelOptionMapValue() {
        Serde<ProjectClassLabelOptionMap> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }


    public Serde<ProjectFieldLanguageKey> ProjectPropertyLanguageKey() {
        Serde<ProjectFieldLanguageKey> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }

    public Serde<ProjectFieldLanguageValue> ProjectFieldLanguageValue() {
        Serde<ProjectFieldLanguageValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<ProjectPropertyLanguageValue> ProjectPropertyLanguageValue() {
        Serde<ProjectPropertyLanguageValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }


    public Serde<ProjectFieldLabelKey> ProjectPropertyLabelKey() {
        Serde<ProjectFieldLabelKey> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }

    public Serde<ProjectFieldLabelValue> ProjectPropertyLabelValue() {
        Serde<ProjectFieldLabelValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<ProjectFieldLabelOptionMap> ProjectPropertyLabelOptionMapValue() {
        Serde<ProjectFieldLabelOptionMap> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<ProjectEntityKey> ProjectEntityKey() {
        Serde<ProjectEntityKey> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }

    public Serde<ProjectEntityValue> ProjectEntityValue() {
        Serde<ProjectEntityValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<ts.information.language.Key> InfLanguageKey() {
        Serde<ts.information.language.Key> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }


    public Serde<ts.information.language.Value> InfLanguageValue() {
        Serde<ts.information.language.Value> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<ts.information.appellation.Key> InfAppellationKey() {
        Serde<ts.information.appellation.Key> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }

    public Serde<ts.information.appellation.Value> InfAppellationValue() {
        Serde<ts.information.appellation.Value> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<ts.information.lang_string.Key> InfLangStringKey() {
        Serde<ts.information.lang_string.Key> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }

    public Serde<ts.information.lang_string.Value> InfLangStringValue() {
        Serde<ts.information.lang_string.Value> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<ts.information.place.Key> InfPlaceKey() {
        Serde<ts.information.place.Key> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }

    public Serde<ts.information.place.Value> InfPlaceValue() {
        Serde<ts.information.place.Value> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<ts.information.time_primitive.Key> InfTimePrimitiveKey() {
        Serde<ts.information.time_primitive.Key> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }

    public Serde<ts.information.time_primitive.Value> InfTimePrimitiveValue() {
        Serde<ts.information.time_primitive.Value> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<ts.information.dimension.Key> InfDimensionKey() {
        Serde<ts.information.dimension.Key> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }

    public Serde<ts.information.dimension.Value> InfDimensionValue() {
        Serde<ts.information.dimension.Value> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<ts.information.statement.Key> InfStatementKey() {
        Serde<ts.information.statement.Key> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }

    public Serde<ts.information.statement.Value> InfStatementValue() {
        Serde<ts.information.statement.Value> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<NodeKey> NodeKey() {
        Serde<NodeKey> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }

    public Serde<NodeValue> NodeValue() {
        Serde<NodeValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<StatementEnrichedValue> StatementEnrichedValue() {
        Serde<StatementEnrichedValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<ts.data.digital.Key> DatDigitalKey() {
        Serde<ts.data.digital.Key> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }

    public Serde<ts.data.digital.Value> DatDigitalValue() {
        Serde<ts.data.digital.Value> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<ts.tables.cell.Key> TabCellKey() {
        Serde<ts.tables.cell.Key> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }

    public Serde<ts.tables.cell.Value> TabCellValue() {
        Serde<ts.tables.cell.Value> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<ProjectStatementKey> ProjectStatementKey() {
        Serde<ProjectStatementKey> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }

    public Serde<ProjectStatementValue> ProjectStatementValue() {
        Serde<ProjectStatementValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<ts.projects.entity_label_config.Key> ProEntityLabelConfigKey() {
        Serde<ts.projects.entity_label_config.Key> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }

    public Serde<ts.projects.entity_label_config.Value> ProEntityLabelConfigValue() {
        Serde<ts.projects.entity_label_config.Value> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<CommunityEntityLabelConfigKey> CommunityEntityLabelConfigKey() {
        Serde<CommunityEntityLabelConfigKey> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }

    public Serde<CommunityEntityLabelConfigValue> CommunityEntityLabelConfigValue() {
        Serde<CommunityEntityLabelConfigValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<ProjectEntityLabelConfigValue> ProjectEntityLabelConfigValue() {
        Serde<ProjectEntityLabelConfigValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }


    public Serde<ProjectTopStatementsKey> ProjectTopStatementsKey() {
        Serde<ProjectTopStatementsKey> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }

    public Serde<ProjectTopStatementsValue> ProjectTopStatementsValue() {
        Serde<ProjectTopStatementsValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<ProjectEntityWithConfigValue> ProjectEntityWithConfigValue() {
        Serde<ProjectEntityWithConfigValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<ProjectEntityLabelPartKey> ProjectEntityLabelPartKey() {
        Serde<ProjectEntityLabelPartKey> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }

    public Serde<ProjectEntityLabelPartValue> ProjectEntityLabelPartValue() {
        Serde<ProjectEntityLabelPartValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<EntityLabelSlotWithStringValue> ProjectEntityLabelSlotWithStringValue() {
        Serde<EntityLabelSlotWithStringValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<ProjectEntityLabelValue> ProjectEntityLabelValue() {
        Serde<ProjectEntityLabelValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<ProjectEntityFulltextValue> ProjectEntityFulltextValue() {
        Serde<ProjectEntityFulltextValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }


    public Serde<TimeSpanValue> TimeSpanValue() {
        Serde<TimeSpanValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<HasTypePropertyKey> HasTypePropertyKey() {
        Serde<HasTypePropertyKey> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }

    public Serde<HasTypePropertyValue> HasTypePropertyValue() {
        Serde<HasTypePropertyValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<HasTypePropertyGroupByValue> HasTypePropertyGroupByValue() {
        Serde<HasTypePropertyGroupByValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<HasTypePropertyAggregateValue> HasTypePropertyAggregateValue() {
        Serde<HasTypePropertyAggregateValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<ProjectEntityHasTypePropValue> ProjectEntityHasTypePropValue() {
        Serde<ProjectEntityHasTypePropValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<ProjectEntityTypeValue> ProjectEntityTypeValue() {
        Serde<ProjectEntityTypeValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }


    public Serde<ProjectEntityClassLabelValue> ProjectEntityClassLabelValue() {
        Serde<ProjectEntityClassLabelValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<EntityPreviewValue> EntityPreviewValue() {
        Serde<EntityPreviewValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<OntomeClassKey> OntomeClassKey() {
        Serde<OntomeClassKey> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }

    public Serde<OntomeClassValue> OntomeClassValue() {
        Serde<OntomeClassValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<OntomeClassMetadataValue> OntomeClassMetadataValue() {
        Serde<OntomeClassMetadataValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<ProjectEntityClassMetadataValue> ProjectEntityClassMetadataValue() {
        Serde<ProjectEntityClassMetadataValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }


    public Serde<OntomePropertyKey> OntomePropertyKey() {
        Serde<OntomePropertyKey> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }

    public Serde<OntomePropertyValue> OntomePropertyValue() {
        Serde<OntomePropertyValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<FieldChangeJoin> FieldChangeJoin() {
        Serde<FieldChangeJoin> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<FieldChangeKey> FieldChangeKey() {
        Serde<FieldChangeKey> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }

    public Serde<FieldChangeValue> FieldChangeValue() {
        Serde<FieldChangeValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<CommunityPropertyLabelKey> CommunityPropertyLabelKey() {
        Serde<CommunityPropertyLabelKey> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }

    public Serde<CommunityPropertyLabelValue> CommunityPropertyLabelValue() {
        Serde<CommunityPropertyLabelValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<CommunityEntityKey> CommunityEntityKey() {
        Serde<CommunityEntityKey> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }


    public Serde<CommunityEntityValue> CommunityEntityValue() {
        Serde<CommunityEntityValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<ProjectEntityVisibilityValue> ProjectEntityVisibilityValue() {
        Serde<ProjectEntityVisibilityValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<CommunityStatementKey> CommunityStatementKey() {
        Serde<CommunityStatementKey> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }

    public Serde<CommunityStatementValue> CommunityStatementValue() {
        Serde<CommunityStatementValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<DRMap> DRMap() {
        Serde<DRMap> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<CommunityEntityLabelValue> CommunityEntityLabelValue() {
        Serde<CommunityEntityLabelValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<CommunityTopStatementsKey> CommunityTopStatementsKey() {
        Serde<CommunityTopStatementsKey> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }

    public Serde<CommunityTopStatementsValue> CommunityTopStatementsValue() {
        Serde<CommunityTopStatementsValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<CommunityEntityWithConfigValue> CommunityEntityWithConfigValue() {
        Serde<CommunityEntityWithConfigValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<CommunityEntityLabelPartKey> CommunityEntityLabelPartKey() {
        Serde<CommunityEntityLabelPartKey> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }

    public Serde<CommunityEntityLabelPartValue> CommunityEntityLabelPartValue() {
        Serde<CommunityEntityLabelPartValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<CommunityEntityClassLabelValue> CommunityEntityClassLabelValue() {
        Serde<CommunityEntityClassLabelValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<CommunityEntityClassMetadataValue> CommunityEntityClassMetadataValue() {
        Serde<CommunityEntityClassMetadataValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }


    public Serde<CommunityEntityFulltextValue> CommunityEntityFulltextValue() {
        Serde<CommunityEntityFulltextValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }


    public Serde<CommunityEntityTypeValue> CommunityEntityTypeValue() {
        Serde<CommunityEntityTypeValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<CommunityEntityHasTypePropValue> CommunityEntityHasTypePropValue() {
        Serde<CommunityEntityHasTypePropValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<FieldLabelWithTopLabelsValue> FieldLabelWithTopLabelsValue() {
        Serde<FieldLabelWithTopLabelsValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<ProjectFieldTopLabelsValue> ProjectFieldTopLabelsValue() {
        Serde<ProjectFieldTopLabelsValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<EntityFieldTextMapValue> EntityFieldTextMapValue() {
        Serde<EntityFieldTextMapValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<EntityFieldTextMapWithConfigValue> EntityFieldTextMapWithConfigValue() {
        Serde<EntityFieldTextMapWithConfigValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<CommunityFieldTopLabelsValue> CommunityFieldTopLabelsValue() {
        Serde<CommunityFieldTopLabelsValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<TopTimePrimitives> TopTimePrimitives() {
        Serde<TopTimePrimitives> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }
    public Serde<TopTimePrimitivesMap> TopTimePrimitivesMap() {
        Serde<TopTimePrimitivesMap> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<ProjectRdfKey> ProjectRdfKey() {
        Serde<ProjectRdfKey> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }

    public Serde<ProjectRdfValue> ProjectRdfValue() {
        Serde<ProjectRdfValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }
    public Serde<TextValue> TextValue() {
        Serde<TextValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }



}
