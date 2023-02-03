package org.geovistory.toolbox.streams.topologies;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.app.RegisterOutputTopic;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.Utils;

import java.util.HashMap;


public class ProjectEntityTopStatements {

    public static void main(String[] args) {
        System.out.println(buildStandalone(new StreamsBuilder()).describe());
    }

    public static Topology buildStandalone(StreamsBuilder builder) {
        var registerOutputTopic = new RegisterOutputTopic(builder);

        return addProcessors(
                builder,
                registerOutputTopic.projectEntityTable(),
                registerOutputTopic.projectTopStatementsTable(),
                registerOutputTopic.projectPropertyLabelTable()
        ).builder().build();
    }

    public static ProjectEntityTopStatementsReturnValue addProcessors(
            StreamsBuilder builder,
            KTable<ProjectEntityKey, ProjectEntityValue> projectEntityValueKTable,

            KTable<ProjectTopStatementsKey, ProjectTopStatementsValue> projectTopStatementsTable,
            KTable<ProjectFieldLabelKey, ProjectFieldLabelValue> projectFieldLabelTable
    ) {

        var avroSerdes = new ConfluentAvroSerdes();


        /* STREAM PROCESSORS */
        // 2)
        // LeftJoin join the project entity with the top statements to add the entity class to ProjectTopStatementsWithClass
        var topStatementsWithClassTable = projectTopStatementsTable.leftJoin(
                projectEntityValueKTable,
                v -> ProjectEntityKey.newBuilder()
                        .setProjectId(v.getProjectId())
                        .setEntityId(v.getEntityId())
                        .build(),
                (value1, value2) -> {
                    if (value2 == null) return null;
                    return ProjectTopStatementsWithClassValue.newBuilder()
                            .setProjectId(value1.getProjectId())
                            .setEntityId(value1.getEntityId())
                            .setIsOutgoing(value1.getIsOutgoing())
                            .setStatements(value1.getStatements())
                            .setPropertyId(value1.getPropertyId())
                            .setClassId(value2.getClassId())
                            .build();
                },
                Materialized.<ProjectTopStatementsKey, ProjectTopStatementsWithClassValue, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.project_top_statements_with_class_id)
                        .withKeySerde(avroSerdes.ProjectTopStatementsKey())
                        .withValueSerde(avroSerdes.ProjectTopStatementsWithClassValue())
        );

        // 3)
        // LeftJoin join the property label with the top statements to ProjectTopStatementsWithPropLabelValue
        var topStatementsWithPropLabelTable = topStatementsWithClassTable.leftJoin(
                projectFieldLabelTable,
                v -> ProjectFieldLabelKey.newBuilder()
                        .setClassId(v.getClassId())
                        .setIsOutgoing(v.getIsOutgoing())
                        .setProjectId(v.getProjectId())
                        .setPropertyId(v.getPropertyId())
                        .build(),
                (v, value2) -> ProjectTopStatementsWithPropLabelValue.newBuilder()
                        .setClassId(v.getClassId())
                        .setIsOutgoing(v.getIsOutgoing())
                        .setProjectId(v.getProjectId())
                        .setPropertyId(v.getPropertyId())
                        .setStatements(v.getStatements())
                        .setEntityId(v.getEntityId())
                        .setPropertyLabel(value2 != null ? value2.getLabel() : null)
                        .build(),
                Materialized.<ProjectTopStatementsKey, ProjectTopStatementsWithPropLabelValue, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.project_top_statements_with_prop_label)
                        .withKeySerde(avroSerdes.ProjectTopStatementsKey())
                        .withValueSerde(avroSerdes.ProjectTopStatementsWithPropLabelValue())
        );

        // 4)
        // GroupBy ProjectEntityKey
        var groupedTable = topStatementsWithPropLabelTable
                .toStream()
                .groupBy(
                (key, value) ->
                        ProjectEntityKey.newBuilder()
                                .setEntityId(key.getEntityId())
                                .setProjectId(key.getProjectId())
                                .build(),
                Grouped.with(
                        avroSerdes.ProjectEntityKey(), avroSerdes.ProjectTopStatementsWithPropLabelValue()
                ).withName("project_entity_top_statements_with_prop_label_grouped")
        );
        // 5)
        // Aggregate ProjectEntityTopStatementsValue, where the ProjectTopStatementKey is transformed to a string,
        // to be used as key in a map.
        var aggregatedTable = groupedTable.aggregate(
                () -> ProjectEntityTopStatementsValue.newBuilder()
                        .setProjectId(0)
                        .setClassId(0)
                        .setEntityId("")
                        .setMap(new HashMap<>())
                        .setDeleted$1(false)
                        .build(),
                (aggKey, newValue, aggValue) -> {
                    var key = newValue.getPropertyId() + "_" + (newValue.getIsOutgoing() ? "out" : "in");
                    if (newValue.getStatements().size() == 0) {
                        aggValue.getMap().remove(key);
                    } else {
                        aggValue.getMap().put(key, newValue);
                    }
                    return aggValue;
                },
                Named.as(ProjectEntityTopStatements.inner.TOPICS.project_entity_top_tatements_aggregated),
                Materialized.<ProjectEntityKey, ProjectEntityTopStatementsValue, KeyValueStore<Bytes, byte[]>>as(ProjectEntityTopStatements.inner.TOPICS.project_entity_top_tatements_aggregated)
                        .withKeySerde(avroSerdes.ProjectEntityKey())
                        .withValueSerde(avroSerdes.ProjectEntityTopStatementsValue())
        );


        var aggregatedStream = aggregatedTable.toStream();

        /* SINK PROCESSORS */
        aggregatedStream.to(output.TOPICS.project_entity_top_statements,
                Produced.with(avroSerdes.ProjectEntityKey(), avroSerdes.ProjectEntityTopStatementsValue()));

        return new ProjectEntityTopStatementsReturnValue(builder, aggregatedTable, aggregatedStream);

    }


    public enum input {
        TOPICS;
        public final String project_top_statements = ProjectTopStatements.output.TOPICS.project_top_statements;
        public final String project_entity = ProjectEntity.output.TOPICS.project_entity;
        public final String project_property_label = ProjectPropertyLabel.output.TOPICS.project_property_label;

    }

    public enum inner {
        TOPICS;
        public final String project_top_statements_with_class_id = Utils.tsPrefixed("project_top_statements_with_class_id");
        public final String project_top_statements_with_prop_label = Utils.tsPrefixed("project_top_statements_with_prop_label");
        public final String project_entity_top_tatements_aggregated = Utils.tsPrefixed("project_entity_top_tatements_aggregated");

    }

    public enum output {
        TOPICS;
        public final String project_entity_top_statements = Utils.tsPrefixed("project_entity_top_statements");
    }

}
