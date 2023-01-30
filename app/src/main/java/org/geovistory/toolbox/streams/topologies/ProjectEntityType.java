package org.geovistory.toolbox.streams.topologies;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.app.RegisterOutputTopic;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.Utils;


public class ProjectEntityType {

    public static void main(String[] args) {
        System.out.println(buildStandalone(new StreamsBuilder()).describe());
    }

    public static Topology buildStandalone(StreamsBuilder builder) {
        var registerOutputTopic = new RegisterOutputTopic(builder);

        return addProcessors(
                builder,
                registerOutputTopic.projectEntityTable(),
                registerOutputTopic.hasTypePropertyTable(),
                registerOutputTopic.projectTopOutgoingStatementsTable()
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
                Materialized.<ProjectEntityKey, ProjectEntityTypeValue, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.project_entity_with_has_type_statement)
                        .withKeySerde(avroSerdes.ProjectEntityKey())
                        .withValueSerde(avroSerdes.ProjectEntityTypeValue())
        );

        var projectEntityTypeStream = projectEntityTypeTable.toStream();
        /* SINK PROCESSORS */

        projectEntityTypeStream.to(output.TOPICS.project_entity_type,
                Produced.with(avroSerdes.ProjectEntityKey(), avroSerdes.ProjectEntityTypeValue()));

        return new ProjectEntityTypeReturnValue(builder, projectEntityTypeStream);

    }


    public enum input {
        TOPICS;
        public final String project_entity = ProjectEntity.output.TOPICS.project_entity;
        public final String has_type_property = HasTypeProperty.output.TOPICS.has_type_property;
        public final String project_top_outgoing_statements = ProjectTopOutgoingStatements.output.TOPICS.project_top_outgoing_statements;
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
