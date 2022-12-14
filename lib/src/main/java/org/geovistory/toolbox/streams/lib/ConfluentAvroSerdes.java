package org.geovistory.toolbox.streams.lib;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.geovistory.toolbox.streams.avro.*;


public class ConfluentAvroSerdes {

    public Serde<dev.projects.dfh_profile_proj_rel.Key> ProProfileProjRelKey() {
        Serde<dev.projects.dfh_profile_proj_rel.Key> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }

    public Serde<dev.projects.dfh_profile_proj_rel.Value> ProProfileProjRelValue() {
        Serde<dev.projects.dfh_profile_proj_rel.Value> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<dev.projects.info_proj_rel.Key> ProInfoProjRelKey() {
        Serde<dev.projects.info_proj_rel.Key> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }

    public Serde<dev.projects.info_proj_rel.Value> ProInfoProjRelValue() {
        Serde<dev.projects.info_proj_rel.Value> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<dev.information.resource.Key> InfResourceKey() {
        Serde<dev.information.resource.Key> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }

    public Serde<dev.information.resource.Value> InfResourceValue() {
        Serde<dev.information.resource.Value> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }


    public Serde<dev.data_for_history.api_property.Key> DfhApiPropertyKey() {
        Serde<dev.data_for_history.api_property.Key> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }

    public Serde<dev.data_for_history.api_property.Value> DfhApiPropertyValue() {
        Serde<dev.data_for_history.api_property.Value> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<dev.data_for_history.api_class.Key> DfhApiClassKey() {
        Serde<dev.data_for_history.api_class.Key> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }

    public Serde<dev.data_for_history.api_class.Value> DfhApiClassValue() {
        Serde<dev.data_for_history.api_class.Value> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<dev.projects.project.Key> ProProjectKey() {
        Serde<dev.projects.project.Key> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }

    public Serde<dev.projects.project.Value> ProProjectValue() {
        Serde<dev.projects.project.Value> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<dev.system.config.Key> SysConfigKey() {
        Serde<dev.system.config.Key> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }

    public Serde<dev.system.config.Value> SysConfigValue() {
        Serde<dev.system.config.Value> serdes = new SpecificAvroSerde<>();
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

    public Serde<ProfileProperty> ProfilePropertyValue() {
        Serde<ProfileProperty> serdes = new SpecificAvroSerde<>();
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

    public Serde<dev.projects.text_property.Key> ProTextPropertyKey() {
        Serde<dev.projects.text_property.Key> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }

    public Serde<dev.projects.text_property.Value> ProTextPropertyValue() {
        Serde<dev.projects.text_property.Value> serdes = new SpecificAvroSerde<>();
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

    public Serde<dev.information.language.Key> InfLanguageKey() {
        Serde<dev.information.language.Key> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }


    public Serde<dev.information.language.Value> InfLanguageValue() {
        Serde<dev.information.language.Value> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<dev.information.appellation.Key> InfAppellationKey() {
        Serde<dev.information.appellation.Key> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }

    public Serde<dev.information.appellation.Value> InfAppellationValue() {
        Serde<dev.information.appellation.Value> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<dev.information.lang_string.Key> InfLangStringKey() {
        Serde<dev.information.lang_string.Key> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }

    public Serde<dev.information.lang_string.Value> InfLangStringValue() {
        Serde<dev.information.lang_string.Value> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<dev.information.place.Key> InfPlaceKey() {
        Serde<dev.information.place.Key> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }

    public Serde<dev.information.place.Value> InfPlaceValue() {
        Serde<dev.information.place.Value> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<dev.information.time_primitive.Key> InfTimePrimitiveKey() {
        Serde<dev.information.time_primitive.Key> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }

    public Serde<dev.information.time_primitive.Value> InfTimePrimitiveValue() {
        Serde<dev.information.time_primitive.Value> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<dev.information.dimension.Key> InfDimensionKey() {
        Serde<dev.information.dimension.Key> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }

    public Serde<dev.information.dimension.Value> InfDimensionValue() {
        Serde<dev.information.dimension.Value> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<dev.information.statement.Key> InfStatementKey() {
        Serde<dev.information.statement.Key> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }

    public Serde<dev.information.statement.Value> InfStatementValue() {
        Serde<dev.information.statement.Value> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<LiteralKey> LiteralKey() {
        Serde<LiteralKey> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }

    public Serde<LiteralValue> LiteralValue() {
        Serde<LiteralValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<StatementEnrichedKey> StatementEnrichedKey() {
        Serde<StatementEnrichedKey> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }

    public Serde<StatementEnrichedValue> StatementEnrichedValue() {
        Serde<StatementEnrichedValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<dev.data.digital.Key> DatDigitalKey() {
        Serde<dev.data.digital.Key> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }

    public Serde<dev.data.digital.Value> DatDigitalValue() {
        Serde<dev.data.digital.Value> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<dev.tables.cell.Key> TabCellKey() {
        Serde<dev.tables.cell.Key> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }

    public Serde<dev.tables.cell.Value> TabCellValue() {
        Serde<dev.tables.cell.Value> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

}