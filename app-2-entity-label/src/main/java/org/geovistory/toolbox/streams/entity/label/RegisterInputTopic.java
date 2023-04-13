package org.geovistory.toolbox.streams.entity.label;

import org.apache.kafka.streams.kstream.KTable;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.lib.TsRegisterInputTopic;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

/**
 * This class provides helper methods to register
 * source topics (topics consumed but not generated by this app)
 */
@ApplicationScoped
public class RegisterInputTopic extends TsRegisterInputTopic {

    @Inject
    AvroSerdes avroSerdes;
    @Inject
    public BuilderSingleton builderSingleton;

    @Inject
    public InputTopicNames inputTopicNames;


    public RegisterInputTopic(AvroSerdes avroSerdes, BuilderSingleton builderSingleton, InputTopicNames inputTopicNames) {
        this.avroSerdes = avroSerdes;
        this.builderSingleton = builderSingleton;
        this.inputTopicNames = inputTopicNames;
    }

    public KTable<dev.projects.info_proj_rel.Key, dev.projects.info_proj_rel.Value> proInfoProjRelTable() {
        return getRepartitionedTable(
                builderSingleton.builder,
                inputTopicNames.proInfoProjRel(),
                avroSerdes.ProInfoProjRelKey(),
                avroSerdes.ProInfoProjRelValue()
        );
    }


    public KTable<dev.information.resource.Key, dev.information.resource.Value> infResourceTable() {
        return getRepartitionedTable(
                builderSingleton.builder,
                inputTopicNames.infResource(),
                avroSerdes.InfResourceKey(),
                avroSerdes.InfResourceValue()
        );
    }


    public KTable<dev.information.statement.Key, StatementEnrichedValue> statementWithLiteralTable() {
        return getRepartitionedTable(
                builderSingleton.builder,
                inputTopicNames.getStatementWithLiteral(),
                avroSerdes.InfStatementKey(), avroSerdes.StatementEnrichedValue());
    }

    public KTable<dev.information.statement.Key, StatementEnrichedValue> statementWithEntityTable() {
        return getRepartitionedTable(
                builderSingleton.builder,
                inputTopicNames.getStatementWithEntity(),
                avroSerdes.InfStatementKey(), avroSerdes.StatementEnrichedValue());
    }


    public KTable<ProjectClassKey, ProjectEntityLabelConfigValue> projectEntityLabelConfigTable() {
        return getRepartitionedTable(
                builderSingleton.builder,
                inputTopicNames.getProjectEntityLabelConfig(),
                avroSerdes.ProjectClassKey(), avroSerdes.ProjectEntityLabelConfigValue());
    }

    public KTable<CommunityEntityLabelConfigKey, CommunityEntityLabelConfigValue> communityEntityLabelConfigTable() {
        return getRepartitionedTable(
                builderSingleton.builder,
                inputTopicNames.getCommunityEntityLabelConfig(),
                avroSerdes.CommunityEntityLabelConfigKey(), avroSerdes.CommunityEntityLabelConfigValue());
    }

}