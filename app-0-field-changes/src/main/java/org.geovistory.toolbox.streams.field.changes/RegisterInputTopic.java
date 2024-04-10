package org.geovistory.toolbox.streams.field.changes;

import ts.projects.info_proj_rel.Key;
import ts.projects.info_proj_rel.Value;
import org.apache.kafka.streams.kstream.KTable;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.lib.TopicNameEnum;
import org.geovistory.toolbox.streams.lib.TsRegisterInputTopic;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

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

    @ConfigProperty(name = "ts.input.topic.name.prefix", defaultValue = "ts")
    String prefix;

    RegisterInputTopic() {
    }

    public RegisterInputTopic(AvroSerdes avroSerdes, BuilderSingleton builderSingleton) {
        this.avroSerdes = avroSerdes;
        this.builderSingleton = builderSingleton;
    }

    public KTable<Key, Value> proInfoProjRelTable() {
        return getRepartitionedTable(
                builderSingleton.builder,
                prefixedIn(prefix, TopicNameEnum.pro_info_proj_rel.getValue()),
                avroSerdes.ProInfoProjRelKey(),
                avroSerdes.ProInfoProjRelValue()
        );
    }

    public KTable<ts.information.statement.Key, ts.information.statement.Value> infStatementTable() {
        return getRepartitionedTable(
                builderSingleton.builder,
                prefixedIn(prefix, TopicNameEnum.inf_statement.getValue()),
                avroSerdes.InfStatementKey(),
                avroSerdes.InfStatementValue()
        );
    }

}