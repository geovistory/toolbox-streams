package org.geovistory.toolbox.streams.entity.processors.project;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.entity.AvroSerdes;
import org.geovistory.toolbox.streams.entity.OutputTopicNames;
import org.geovistory.toolbox.streams.entity.RegisterInputTopic;
import org.geovistory.toolbox.streams.lib.Utils;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;


@ApplicationScoped
public class ProjectEntityType {

    @Inject
    AvroSerdes avroSerdes;

    @Inject
    RegisterInputTopic registerInputTopic;

    @Inject
    OutputTopicNames outputTopicNames;

    public ProjectEntityType(AvroSerdes avroSerdes, RegisterInputTopic registerInputTopic, OutputTopicNames outputTopicNames) {
        this.avroSerdes = avroSerdes;
        this.registerInputTopic = registerInputTopic;
        this.outputTopicNames = outputTopicNames;
    }

    public void addProcessorsStandalone() {

        addProcessors(
                registerInputTopic.projectEntityTable(),
                registerInputTopic.hasTypePropertyTable(),
                registerInputTopic.projectTopOutgoingStatementsTable()
        );
    }

    public ProjectEntityTypeReturnValue addProcessors(
            KTable<ProjectEntityKey, ProjectEntityValue> projectEntityTable,
            KTable<HasTypePropertyKey, HasTypePropertyValue> hasTypePropertyTable,
            KTable<ProjectTopStatementsKey, ProjectTopStatementsValue> projectTopOutgoingStatementsTable) {


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
                TableJoined.as(inner.TOPICS.project_entity_with_has_type_property + "-fk-join"),
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
                    var edges = value2.getEdges();
                    var hasTypeStatement = edges.size() == 0 ? null : value2.getEdges().get(0);
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
                                .setTypeId(hasTypeStatement.getTargetId())
                                .setTypeLabel(hasTypeStatement.getTargetLabel())
                                .setDeleted$1(false)
                                .build();
                    }

                },
                TableJoined.as(inner.TOPICS.project_entity_with_has_type_statement + "-fk-join"),
                Materialized.<ProjectEntityKey, ProjectEntityTypeValue, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.project_entity_with_has_type_statement)
                        .withKeySerde(avroSerdes.ProjectEntityKey())
                        .withValueSerde(avroSerdes.ProjectEntityTypeValue())
        );

        var projectEntityTypeStream = projectEntityTypeTable.toStream(
                Named.as(inner.TOPICS.project_entity_with_has_type_statement + "-to-stream")
        );
        /* SINK PROCESSORS */

        projectEntityTypeStream.to(outputTopicNames.projectEntityType(),
                Produced.with(avroSerdes.ProjectEntityKey(), avroSerdes.ProjectEntityTypeValue())
                        .withName(outputTopicNames.projectEntityType() + "-producer")
        );

        return new ProjectEntityTypeReturnValue(projectEntityTypeTable, projectEntityTypeStream);

    }


    public enum inner {
        TOPICS;
        public final String project_entity_with_has_type_property = "project_entity_with_has_type_property";
        public final String project_entity_with_has_type_statement = "project_entity_with_has_type_statement";

    }

}
