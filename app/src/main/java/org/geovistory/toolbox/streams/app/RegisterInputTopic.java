package org.geovistory.toolbox.streams.app;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.Utils;

/**
 * This class provides helper methods to register
 * source topics (topics consumed but not generated by this app)
 */
public class RegisterInputTopic {
    public StreamsBuilder builder;
    public ConfluentAvroSerdes avroSerdes;

    public RegisterInputTopic(StreamsBuilder builder) {
        this.builder = builder;
        this.avroSerdes = new ConfluentAvroSerdes();
    }

    public KTable<dev.projects.project.Key, dev.projects.project.Value> proProjectTable() {
        return builder.table(
                DbTopicNames.pro_projects.getName(),
                Consumed.with(avroSerdes.ProProjectKey(), avroSerdes.ProProjectValue())
        );
    }

    public KTable<dev.projects.text_property.Key, dev.projects.text_property.Value> proTextPropertyTable() {
        return builder.table(
                DbTopicNames.pro_text_property.getName(),
                Consumed.with(avroSerdes.ProTextPropertyKey(), avroSerdes.ProTextPropertyValue())
        );
    }

    public KTable<dev.projects.dfh_profile_proj_rel.Key, dev.projects.dfh_profile_proj_rel.Value> proProfileProjRelTable() {
        return builder.table(
                DbTopicNames.pro_dfh_profile_proj_rel.getName(),
                Consumed.with(avroSerdes.ProProfileProjRelKey(), avroSerdes.ProProfileProjRelValue())
        );
    }

    public KTable<dev.projects.info_proj_rel.Key, dev.projects.info_proj_rel.Value> proInfoProjRelTable() {
        return builder.stream(
                DbTopicNames.pro_info_proj_rel.getName(),
                Consumed.with(avroSerdes.ProInfoProjRelKey(), avroSerdes.ProInfoProjRelValue())
        ).toTable(
                Named.as(Utils.tsPrefixed(DbTopicNames.pro_info_proj_rel.getName())),
                Materialized.with(avroSerdes.ProInfoProjRelKey(), avroSerdes.ProInfoProjRelValue())
        );
    }

    public KStream<dev.projects.entity_label_config.Key, dev.projects.entity_label_config.Value> proEntityLabelConfigStream() {
        return builder.stream(
                DbTopicNames.pro_entity_label_config.getName(),
                Consumed.with(avroSerdes.ProEntityLabelConfigKey(), avroSerdes.ProEntityLabelConfigValue())
        );
    }

    public KTable<dev.information.resource.Key, dev.information.resource.Value> infResourceTable() {
        return builder.stream(
                DbTopicNames.inf_resource.getName(),
                Consumed.with(avroSerdes.InfResourceKey(), avroSerdes.InfResourceValue())
        ).toTable(
                Named.as(Utils.tsPrefixed(DbTopicNames.pro_info_proj_rel.getName())),
                Materialized.with(avroSerdes.InfResourceKey(), avroSerdes.InfResourceValue())
        );
    }

    public KStream<dev.information.language.Key, dev.information.language.Value> infLanguageStream() {
        return builder.stream(
                DbTopicNames.inf_language.getName(),
                Consumed.with(avroSerdes.InfLanguageKey(), avroSerdes.InfLanguageValue())
        );
    }

    public KStream<dev.information.appellation.Key, dev.information.appellation.Value> infAppellationStream() {
        return builder.stream(
                DbTopicNames.inf_appellation.getName(),
                Consumed.with(avroSerdes.InfAppellationKey(), avroSerdes.InfAppellationValue())
        );
    }

    public KStream<dev.information.lang_string.Key, dev.information.lang_string.Value> infLangStringStream() {
        return builder.stream(
                DbTopicNames.inf_lang_string.getName(),
                Consumed.with(avroSerdes.InfLangStringKey(), avroSerdes.InfLangStringValue())
        );
    }

    public KStream<dev.information.place.Key, dev.information.place.Value> infPlaceStream() {
        return builder.stream(
                DbTopicNames.inf_place.getName(),
                Consumed.with(avroSerdes.InfPlaceKey(), avroSerdes.InfPlaceValue())
        );
    }

    public KStream<dev.information.time_primitive.Key, dev.information.time_primitive.Value> infTimePrimitiveStream() {
        return builder.stream(
                DbTopicNames.inf_time_primitive.getName(),
                Consumed.with(avroSerdes.InfTimePrimitiveKey(), avroSerdes.InfTimePrimitiveValue())
        );
    }

    public KStream<dev.information.dimension.Key, dev.information.dimension.Value> infDimensionStream() {
        return builder.stream(
                DbTopicNames.inf_dimension.getName(),
                Consumed.with(avroSerdes.InfDimensionKey(), avroSerdes.InfDimensionValue())
        );
    }

    public KStream<dev.data.digital.Key, dev.data.digital.Value> datDigitalStream() {
        return builder.stream(
                DbTopicNames.dat_digital.getName(),
                Consumed.with(avroSerdes.DatDigitalKey(), avroSerdes.DatDigitalValue())
        );
    }

    public KStream<dev.tables.cell.Key, dev.tables.cell.Value> tabCellStream() {
        return builder.stream(
                DbTopicNames.tab_cell.getName(),
                Consumed.with(avroSerdes.TabCellKey(), avroSerdes.TabCellValue())
        );
    }

    public KTable<dev.information.statement.Key, dev.information.statement.Value> infStatementTable() {
        return builder.table(
                DbTopicNames.inf_statement.getName(),
                Consumed.with(avroSerdes.InfStatementKey(), avroSerdes.InfStatementValue())
        );
    }

    public KTable<dev.system.config.Key, dev.system.config.Value> sysConfigTable() {
        return builder.table(
                DbTopicNames.sys_config.getName(),
                Consumed.with(avroSerdes.SysConfigKey(), avroSerdes.SysConfigValue())
        );
    }

    public KTable<dev.data_for_history.api_property.Key, dev.data_for_history.api_property.Value> dfhApiPropertyTable() {
        return builder.table(
                DbTopicNames.dfh_api_property.getName(),
                Consumed.with(avroSerdes.DfhApiPropertyKey(), avroSerdes.DfhApiPropertyValue())
        );
    }

    public KTable<dev.data_for_history.api_class.Key, dev.data_for_history.api_class.Value> dfhApiClassTable() {
        return builder.table(
                DbTopicNames.dfh_api_class.getName(),
                Consumed.with(avroSerdes.DfhApiClassKey(), avroSerdes.DfhApiClassValue())
        );
    }


}
