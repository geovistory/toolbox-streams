package org.geovistory.toolbox.streams.project.entity.topologies;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.Utils;
import org.geovistory.toolbox.streams.project.entity.Env;
import org.geovistory.toolbox.streams.project.entity.RegisterInnerTopic;
import org.geovistory.toolbox.streams.project.entity.RegisterInputTopic;

import java.util.ArrayList;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.stream.Stream;


public class ProjectEntityFulltext {

    public static void main(String[] args) {
        System.out.println(buildStandalone(new StreamsBuilder()).describe());
    }

    public static Topology buildStandalone(StreamsBuilder builder) {
        var innerTopic = new RegisterInnerTopic(builder);
        var inputTopic = new RegisterInputTopic(builder);

        return addProcessors(
                builder,
                innerTopic.projectEntityTopStatementsTable(),
                inputTopic.projectEntityLabelConfigTable()
        ).builder().build();
    }

    public static ProjectEntityFulltextReturnValue addProcessors(
            StreamsBuilder builder,
            KTable<ProjectEntityKey, ProjectEntityTopStatementsValue> projectEntityTopStatementsTable,
            KTable<ProjectClassKey, ProjectEntityLabelConfigValue> projectLabelConfigTable) {

        var avroSerdes = new ConfluentAvroSerdes();


        /* STREAM PROCESSORS */
        // 2)

        var projectEntityWithConfigTable = projectEntityTopStatementsTable.leftJoin(
                projectLabelConfigTable,
                projectEntityValue -> ProjectClassKey.newBuilder()
                        .setProjectId(projectEntityValue.getProjectId())
                        .setClassId(projectEntityValue.getClassId())
                        .build(),
                (value1, value2) -> ProjectEntityTopStatementsWithConfigValue.newBuilder()
                        .setEntityTopStatements(value1)
                        .setLabelConfig(value2)
                        .build(),
                TableJoined.as(inner.TOPICS.project_entity_top_statements_with_label_config+ "-fk-left-join"),
                Materialized.<ProjectEntityKey, ProjectEntityTopStatementsWithConfigValue, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.project_entity_top_statements_with_label_config)
                        .withKeySerde(avroSerdes.ProjectEntityKey())
                        .withValueSerde(avroSerdes.ProjectEntityTopStatementsWithConfigValue())
        );

        // 3
        var projectEntityFulltextStream = projectEntityWithConfigTable.toStream(
                        Named.as(inner.TOPICS.project_entity_top_statements_with_label_config + "-to-stream")
                )
                .mapValues(
                        (readOnlyKey, value) -> {
                            var fulltext = createFulltext(value);
                            return ProjectEntityFulltextValue.newBuilder()
                                    .setProjectId(readOnlyKey.getProjectId())
                                    .setEntityId(readOnlyKey.getEntityId())
                                    .setFulltext(fulltext).build();
                        },
                        Named.as("kstream-mapvalues-project-entity-top-statements-with-config-value-to-project-entity-fulltext-value")
                );

        /* SINK PROCESSORS */

        projectEntityFulltextStream.to(output.TOPICS.project_entity_fulltext,
                Produced.with(avroSerdes.ProjectEntityKey(), avroSerdes.ProjectEntityFulltextValue())
                        .withName(output.TOPICS.project_entity_fulltext + "-producer")
        );

        return new ProjectEntityFulltextReturnValue(builder, projectEntityFulltextStream);

    }


    public enum input {
        TOPICS;
        public final String project_entity_label_config_enriched = Env.INSTANCE.TOPIC_PROJECT_ENTITY_LABEL_CONFIG;
        public final String project_entity_top_statements = ProjectEntityTopStatements.output.TOPICS.project_entity_top_statements;
    }


    public enum inner {
        TOPICS;
        public final String project_entity_top_statements_with_label_config = "project_entity_top_statements_with_label_config";

    }

    public enum output {
        TOPICS;
        public final String project_entity_fulltext = Utils.tsPrefixed("project_entity_fulltext");
    }

    /**
     * creates the fulltext for an entity
     *
     * @param v a ProjectEntityTopStatementsWithConfigValue
     * @return Fulltext string.
     */
    public static String createFulltext(ProjectEntityTopStatementsWithConfigValue v) {
        var strings = new ArrayList<String>();
        var topStatements = v.getEntityTopStatements();
        if (topStatements == null) return "";
        var topStatementsMap = v.getEntityTopStatements().getMap();
        var labelConfig = v.getLabelConfig();
        String entityLabel = getEntityLabel(topStatementsMap);

        // process fields from label config
        if (labelConfig != null) {
            labelConfig.getConfig().getLabelParts().forEach(entityLabelConfigPart -> {
                var f = entityLabelConfigPart.getField();
                var s = createFieldText(topStatementsMap, f.getIsOutgoing(), f.getFkProperty());
                if (s != null) strings.add(s);

                // remove key from map
                topStatementsMap.remove(getFieldKey(f.getIsOutgoing(), f.getFkProperty()));

            });
        }

        // process rest of fields
        topStatementsMap.forEach((key, value) -> {
            var s = createFieldText(topStatementsMap, value.getIsOutgoing(), value.getPropertyId());
            if (s != null) strings.add(s);
        });

        var fieldsText = String.join(".\n", strings);

        var parts = Stream.of(entityLabel, fieldsText)
                .filter(s -> s != null && !Objects.equals(s, ""))
                .toList();

        if (parts.size() == 0) return "";

        return String.join("\n", parts) + ".";
    }

    private static String getEntityLabel(Map<String, ProjectTopStatementsWithPropLabelValue> topStatementsMap) {
        try {
            var firstField = topStatementsMap.entrySet().iterator().next().getValue();
            var firstStatement = firstField.getStatements().get(0).getStatement();
            return firstField.getIsOutgoing() ?
                    firstStatement.getSubjectLabel() : firstStatement.getObjectLabel();
        } catch (NoSuchElementException e) {
            return null;
        }
    }

    private static String createFieldText(Map<String, ProjectTopStatementsWithPropLabelValue> topStatementsMap,
                                          boolean isOutgoing, int propertyId) {
        String key = getFieldKey(isOutgoing, propertyId);
        var topStatements = topStatementsMap.get(key);
        var fieldStrings = new ArrayList<String>();
        topStatements.getStatements().forEach(s -> {
            var stmt = s.getStatement();
            var targetLabel = isOutgoing ? stmt.getObjectLabel() : stmt.getSubjectLabel();
            if (targetLabel != null && !targetLabel.equals("")) fieldStrings.add(targetLabel);
        });

        if (fieldStrings.size() > 0) {
            var propertyLabel = topStatements.getPropertyLabel();
            var targetLabels = String.join(", ", fieldStrings);
            if (propertyLabel != null && !Objects.equals(propertyLabel, "")) {
                return propertyLabel + ": " + targetLabels;
            }
            return targetLabels;
        }
        return null;
    }

    private static String getFieldKey(boolean isOutgoing, int propertyId) {
        return propertyId + "_" + (isOutgoing ? "out" : "in");
    }
}
