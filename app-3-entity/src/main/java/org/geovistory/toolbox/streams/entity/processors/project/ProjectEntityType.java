package org.geovistory.toolbox.streams.entity.processors.project;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.entity.Env;
import org.geovistory.toolbox.streams.entity.RegisterInputTopic;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.Utils;


public class ProjectEntityType {

    public static void main(String[] args) {
        System.out.println(buildStandalone(new StreamsBuilder()).describe());
    }

    public static Topology buildStandalone(StreamsBuilder builder) {
        var inputTopic = new RegisterInputTopic(builder);

        return addProcessors(
                builder,
                inputTopic.projectEntityTable(),
                inputTopic.hasTypePropertyTable(),
                inputTopic.projectTopOutgoingStatementsTable()
        ).builder().build();
    }

    public static ProjectEntityTypeReturnValue addProcessors(
            StreamsBuilder builder,
            KTable<ProjectEntityKey, ProjectEntityValue> projectEntityTable,
            KTable<HasTypePropertyKey, HasTypePropertyValue> hasTypePropertyTable,
            KTable<ProjectTopStatementsKey, ProjectTopStatementsValue> projectTopOutgoingStatementsTable) {

        var avroSerdes = new ConfluentAvroSerdes();


        /* STREAM PROCESSORS */
        // 2)

        var projectEntityWithHasTypeProp = projectEntityTable.join(
                hasTypePropertyTable,
                projectEntityValue -> HasTypePropertyKey.newBuilder()
                        .setClassId(projectEntityValue.getClassId())
                        .build(),
                (value1, value2) -> ProjectEntityHasTypePropValue.newBuilder()
                        .setEntityId(value1.getEntityId())
                        .setProjectId(value1.getProjectId())
                        .setHasTypePropertyId(value2.getPropertyId())
                        .setDeleted$1(value2.getDeleted$1())
                        .build(),
                TableJoined.as(inner.TOPICS.project_entity_with_has_type_property+ "-fk-join"),
                Materialized.<ProjectEntityKey, ProjectEntityHasTypePropValue, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.project_entity_with_has_type_property)
                        .withKeySerde(avroSerdes.ProjectEntityKey())
                        .withValueSerde(avroSerdes.ProjectEntityHasTypePropValue())
        );

        // 2)

        var projectEntityTypeTable = projectEntityWithHasTypeProp.join(
                projectTopOutgoingStatementsTable,
                projectEntityValue -> ProjectTopStatementsKey.newBuilder()
                        .setIsOutgoing(true)
                        .setProjectId(projectEntityValue.getProjectId())
                        .setEntityId(projectEntityValue.getEntityId())
                        .setPropertyId(projectEntityValue.getHasTypePropertyId())
                        .build(),
                (value1, value2) -> {
                    var statements = value2.getStatements();
                    var hasTypeStatement = statements.size() == 0 ? null : value2.getStatements().get(0);
                    var deleted = hasTypeStatement == null || Utils.booleanIsEqualTrue(value1.getDeleted$1());
                    var newVal = ProjectEntityTypeValue.newBuilder()
                            .setEntityId(value1.getEntityId())
                            .setProjectId(value1.getProjectId());
                    if (deleted) {
                        return newVal
                                .setTypeId("")
                                .setTypeLabel(null)
                                .setDeleted$1(true)
                                .build();
                    } else {
                        return newVal
                                .setTypeId(hasTypeStatement.getStatement().getObjectId())
                                .setTypeLabel(hasTypeStatement.getStatement().getObjectLabel())
                                .setDeleted$1(false)
                                .build();
                    }

                },
                TableJoined.as(inner.TOPICS.project_entity_with_has_type_statement+ "-fk-join"),
                Materialized.<ProjectEntityKey, ProjectEntityTypeValue, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.project_entity_with_has_type_statement)
                        .withKeySerde(avroSerdes.ProjectEntityKey())
                        .withValueSerde(avroSerdes.ProjectEntityTypeValue())
        );

        var projectEntityTypeStream = projectEntityTypeTable.toStream(
                Named.as(inner.TOPICS.project_entity_with_has_type_statement + "-to-stream")
        );
        /* SINK PROCESSORS */

        projectEntityTypeStream.to(output.TOPICS.project_entity_type,
                Produced.with(avroSerdes.ProjectEntityKey(), avroSerdes.ProjectEntityTypeValue())
                        .withName(output.TOPICS.project_entity_type + "-producer")
        );

        return new ProjectEntityTypeReturnValue(builder, projectEntityTypeTable, projectEntityTypeStream);

    }


    public enum input {
        TOPICS;
        public final String project_entity = Env.INSTANCE.TOPIC_PROJECT_ENTITY;
        public final String has_type_property = Env.INSTANCE.TOPIC_HAS_TYPE_PROPERTY;
        public final String project_top_outgoing_statements = Env.INSTANCE.TOPIC_PROJECT_TOP_OUTGOING_STATEMENTS;
    }


    public enum inner {
        TOPICS;
        public final String project_entity_with_has_type_property = "project_entity_with_has_type_property";
        public final String project_entity_with_has_type_statement = "project_entity_with_has_type_statement";

    }

    public enum output {
        TOPICS;
        public final String project_entity_type = Utils.tsPrefixed("project_entity_type");
    }


}
