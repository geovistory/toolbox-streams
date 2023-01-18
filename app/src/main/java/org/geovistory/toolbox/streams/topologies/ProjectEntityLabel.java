package org.geovistory.toolbox.streams.topologies;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.app.RegisterOutputTopic;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.Utils;

import java.util.*;


public class ProjectEntityLabel {

    public static final int NUMBER_OF_SLOTS = 10;
    public static final int MAX_STRING_LENGTH = 100;

    public static void main(String[] args) {
        System.out.println(buildStandalone(new StreamsBuilder()).describe());
    }

    public static Topology buildStandalone(StreamsBuilder builder) {
        var registerOutputTopic = new RegisterOutputTopic(builder);

        return addProcessors(
                builder,
                registerOutputTopic.projectEntityTable(),
                registerOutputTopic.projectEntityLabelConfigTable(),
                registerOutputTopic.projectTopStatementsTable()
        ).builder().build();
    }

    public static ProjectEntityLabelReturnValue addProcessors(
            StreamsBuilder builder,
            KTable<ProjectEntityKey, ProjectEntityValue> projectEntityTable,
            KTable<ProjectClassKey, ProjectEntityLabelConfigValue> projectLabelConfigTable,
            KTable<ProjectTopStatementsKey, ProjectTopStatementsValue> projectTopStatementsTable) {

        var avroSerdes = new ConfluentAvroSerdes();


        /* STREAM PROCESSORS */
        // 2)

        var projectEntityWithConfigTable = projectEntityTable.leftJoin(
                projectLabelConfigTable,
                projectEntityValue -> ProjectClassKey.newBuilder()
                        .setProjectId(projectEntityValue.getProjectId())
                        .setClassId(projectEntityValue.getClassId())
                        .build(),
                (value1, value2) -> ProjectEntityWithConfigValue.newBuilder()
                        .setEntity(value1)
                        .setLabelConfig(value2)
                        .build(),
                Materialized.<ProjectEntityKey, ProjectEntityWithConfigValue, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.project_entity_with_label_config)
                        .withKeySerde(avroSerdes.ProjectEntityKey())
                        .withValueSerde(avroSerdes.ProjectEntityWithConfigValue())
        );

        // 3
        var projectEntityLabelSlots = projectEntityWithConfigTable
                .toStream()
                .flatMap((key, value) -> {
                    List<KeyValue<ProjectEntityLabelPartKey, ProjectEntityLabelPartValue>> result = new LinkedList<>();

                    if (value.getLabelConfig() == null) return result;
                    if (value.getLabelConfig().getConfig() == null) return result;
                    if (value.getLabelConfig().getConfig().getLabelParts() == null) return result;

                    var labelParts = value.getLabelConfig().getConfig().getLabelParts();
                    // sort parts according to ord num
                    labelParts.sort(Comparator.comparingInt(EntityLabelConfigPart::getOrdNum));

                    // create  slots, be they configured or not
                    for (int i = 0; i < NUMBER_OF_SLOTS; i++) {
                        var k = ProjectEntityLabelPartKey.newBuilder()
                                .setEntityId(key.getEntityId())
                                .setProjectId(key.getProjectId())
                                .setOrdNum(i)
                                .build();

                        // if no config for slot, then v = null
                        ProjectEntityLabelPartValue v = null;
                        // else assign the value
                        if (labelParts.size() > i && labelParts.get(i) != null && labelParts.get(i).getField() != null) {
                            v = ProjectEntityLabelPartValue.newBuilder()
                                    .setEntityId(key.getEntityId())
                                    .setProjectId(key.getProjectId())
                                    .setOrdNum(i)
                                    .setConfiguration(labelParts.get(i).getField())
                                    .build();
                        }
                        result.add(KeyValue.pair(k, v));
                    }

                    return result;
                });

        // 4
        var projectEntityLabelSlotsTable = projectEntityLabelSlots.toTable(
                Materialized.<ProjectEntityLabelPartKey, ProjectEntityLabelPartValue, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.project_entity_label_slots)
                        .withKeySerde(avroSerdes.ProjectEntityLabelPartKey())
                        .withValueSerde(avroSerdes.ProjectEntityLabelPartValue())
        );
        var projectEntityLabelSlotsWithStrings = projectEntityLabelSlotsTable.leftJoin(
                projectTopStatementsTable,
                (v) -> ProjectTopStatementsKey.newBuilder()
                        .setEntityId(v.getEntityId())
                        .setProjectId(v.getProjectId())
                        .setIsOutgoing(v.getConfiguration().getIsOutgoing())
                        .setPropertyId(v.getConfiguration().getFkProperty())
                        .build(),
                (entityLabelSlot, topStatements) -> {
                    var config = entityLabelSlot.getConfiguration();
                    var result = ProjectEntityLabelSlotWithStringValue.newBuilder()
                            .setString("")
                            .setDeleted$1(true)
                            .setOrdNum(entityLabelSlot.getOrdNum())
                            .build();
                    if (topStatements == null) return result;
                    if (topStatements.getStatements() == null) return result;
                    if (topStatements.getStatements().size() == 0) return result;

                    result.setDeleted$1(false);

                    // get the list of relevant statements
                    var relevantStmts = topStatements.getStatements();
                    var maxSize = config.getNrOfStatementsInLabel();
                    if (relevantStmts.size() > maxSize && maxSize > 0) {
                        relevantStmts = relevantStmts.subList(0, maxSize);
                    }

                    // concatenate the strings
                    var list = relevantStmts
                            .stream()
                            .map(projectStatementValue -> {
                                if (config.getIsOutgoing())
                                    return projectStatementValue.getStatement().getObjectLabel();
                                else return projectStatementValue.getStatement().getSubjectLabel();
                            })
                            .filter(Objects::nonNull)
                            .toList();
                    var slotLabel = String.join(", ", list);

                    if (slotLabel.length() > MAX_STRING_LENGTH) {
                        slotLabel = slotLabel.substring(0, MAX_STRING_LENGTH);
                    }

                    result.setString(slotLabel);
                    return result;
                },
                Materialized.<ProjectEntityLabelPartKey, ProjectEntityLabelSlotWithStringValue, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.project_entity_label_slots_with_strings)
                        .withKeySerde(avroSerdes.ProjectEntityLabelPartKey())
                        .withValueSerde(avroSerdes.ProjectEntityLabelSlotWithStringValue())
        );

        // 5
        var grouped = projectEntityLabelSlotsWithStrings.groupBy(
                (key, value) -> KeyValue.pair(
                        ProjectEntityKey.newBuilder()
                                .setEntityId(key.getEntityId())
                                .setProjectId(key.getProjectId())
                                .build(),
                        value
                ),
                Grouped.with(avroSerdes.ProjectEntityKey(), avroSerdes.ProjectEntityLabelSlotWithStringValue())
                        .withName(output.TOPICS.project_entity_label)
        );
        var aggregated = grouped.aggregate(
                () -> {
                    ArrayList<String> stringList = new ArrayList<>();
                    for (int i = 0; i < NUMBER_OF_SLOTS; i++) {
                        stringList.add("");
                    }
                    return ProjectEntityLabelValue.newBuilder()
                            .setProjectId(0)
                            .setEntityId("")
                            .setLabel("")
                            .setLabelSlots(stringList)
                            .build();
                },
                (aggKey, newValue, aggValue) -> {
                    aggValue.setProjectId(aggKey.getProjectId());
                    aggValue.setEntityId(aggKey.getEntityId());

                    var slots = aggValue.getLabelSlots();
                    var slotNum = newValue.getOrdNum();

                    if (!newValue.getDeleted$1()) slots.set(slotNum, newValue.getString());
                    else slots.set(slotNum, "");

                    var strings = slots.stream().filter(s -> !Objects.equals(s, "")).toList();
                    var entityLabel = String.join(", ", strings);

                    if (entityLabel.length() > MAX_STRING_LENGTH) {
                        entityLabel = entityLabel.substring(0, MAX_STRING_LENGTH);
                    }

                    aggValue.setLabel(entityLabel);

                    return aggValue;
                },
                (aggKey, oldValue, aggValue) -> aggValue,
                Materialized.<ProjectEntityKey, ProjectEntityLabelValue, KeyValueStore<Bytes, byte[]>>as(output.TOPICS.project_entity_label)
                        .withKeySerde(avroSerdes.ProjectEntityKey())
                        .withValueSerde(avroSerdes.ProjectEntityLabelValue())
        );

        var aggregatedStream = aggregated.toStream();
        /* SINK PROCESSORS */

        aggregatedStream.to(output.TOPICS.project_entity_label,
                Produced.with(avroSerdes.ProjectEntityKey(), avroSerdes.ProjectEntityLabelValue()));

        return new ProjectEntityLabelReturnValue(builder, aggregated, aggregatedStream);

    }


    public enum input {
        TOPICS;
        public final String project_entity = ProjectEntity.output.TOPICS.project_entity;
        public final String project_entity_label_config_enriched = ProjectEntityLabelConfig.output.TOPICS.project_entity_label_config_enriched;
        public final String project_top_statements = ProjectTopStatements.output.TOPICS.project_top_statements;
    }


    public enum inner {
        TOPICS;
        public final String project_entity_with_label_config = "project_entity_with_label_config";
        public final String project_entity_label_slots = "project_entity_label_slots";
        public final String project_entity_label_slots_with_strings = "project_entity_label_slots_with_strings";

    }

    public enum output {
        TOPICS;
        public final String project_entity_label = Utils.tsPrefixed("project_entity_label");
    }

}
