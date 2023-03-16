package org.geovistory.toolbox.streams.entity.label.processsors.project;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.entity.label.Env;
import org.geovistory.toolbox.streams.entity.label.RegisterInnerTopic;
import org.geovistory.toolbox.streams.entity.label.RegisterInputTopics;
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
        var registerInputTopic = new RegisterInputTopics(builder);
        var registerInnerTopic = new RegisterInnerTopic(builder);

        return addProcessors(
                builder,
                registerInnerTopic.projectEntityTable(),
                registerInputTopic.projectEntityLabelConfigTable(),
                registerInnerTopic.projectTopStatementsTable()
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
                TableJoined.as(inner.TOPICS.project_entity_with_label_config + "-fk-left-join"),
                Materialized.<ProjectEntityKey, ProjectEntityWithConfigValue, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.project_entity_with_label_config)
                        .withKeySerde(avroSerdes.ProjectEntityKey())
                        .withValueSerde(avroSerdes.ProjectEntityWithConfigValue())
        );

        // 3
        var projectEntityWithConfigStream = projectEntityWithConfigTable
                .toStream(
                        Named.as(inner.TOPICS.project_entity_with_label_config + "-to-stream")
                );
        var projectEntityLabelSlots = projectEntityWithConfigStream
                .flatMap(
                        (key, value) -> {
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
                        },
                        Named.as("kstream-flatmap-project-entity-with-config-to-project-entity-label-slots")
                );

        // 4
        var projectEntityLabelSlotsTable = projectEntityLabelSlots.toTable(
                Named.as(inner.TOPICS.project_entity_label_slots + "-to-table"),
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
                    var result = EntityLabelSlotWithStringValue.newBuilder()
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
                TableJoined.as(inner.TOPICS.project_entity_label_slots_with_strings + "-fk-left-join"),
                Materialized.<ProjectEntityLabelPartKey, EntityLabelSlotWithStringValue, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.project_entity_label_slots_with_strings)
                        .withKeySerde(avroSerdes.ProjectEntityLabelPartKey())
                        .withValueSerde(avroSerdes.ProjectEntityLabelSlotWithStringValue())
        );


        var aggregatedStream = projectEntityLabelSlotsWithStrings
                .toStream(
                        Named.as(inner.TOPICS.project_entity_label_slots_with_strings + "-to-stream")
                )
                .selectKey((k, v) -> ProjectEntityKey.newBuilder()
                                .setProjectId(k.getProjectId())
                                .setEntityId(k.getEntityId())
                                .build(),
                        Named.as("kstream-select-key-of-project-entity-label-slots")
                )
                .repartition(
                        Repartitioned.<ProjectEntityKey, EntityLabelSlotWithStringValue>as(inner.TOPICS.project_entity_label_slots_with_strings_repart)
                                .withKeySerde(avroSerdes.ProjectEntityKey())
                                .withValueSerde(avroSerdes.ProjectEntityLabelSlotWithStringValue())
                                .withName(inner.TOPICS.project_entity_label_slots_with_strings_repart)
                )
                .transform(new EntityLabelsAggregatorSupplier("project_entity_labels_agg"));

        /* SINK PROCESSORS */

        aggregatedStream.to(output.TOPICS.project_entity_label,
                Produced.with(avroSerdes.ProjectEntityKey(), avroSerdes.ProjectEntityLabelValue())
                        .withName(output.TOPICS.project_entity_label + "-producer")
        );

        projectEntityWithConfigStream
                .mapValues((readOnlyKey, value) -> value.getLabelConfig())
                .to(output.TOPICS.project_entity_with_label_config,
                        Produced.with(avroSerdes.ProjectEntityKey(), avroSerdes.ProjectEntityLabelConfigValue())
                                .withName(output.TOPICS.project_entity_with_label_config + "-producer")
                );

        return new ProjectEntityLabelReturnValue(builder, aggregatedStream);

    }


    public enum input {
        TOPICS;
        public final String project_entity = ProjectEntity.output.TOPICS.project_entity;
        public final String project_entity_label_config = Env.INSTANCE.TOPIC_PROJECT_ENTITY_LABEL_CONFIG;
        public final String project_top_statements = ProjectTopStatements.output.TOPICS.project_top_statements;
    }


    public enum inner {
        TOPICS;
        public final String project_entity_with_label_config = "project_entity_with_label_config";
        public final String project_entity_label_slots = "project_entity_label_slots";
        public final String project_entity_label_slots_with_strings = "project_entity_label_slots_with_strings";
        public final String project_entity_label_slots_with_strings_repart = "project_entity_label_slots_with_strings_repart";

    }

    public enum output {
        TOPICS;
        public final String project_entity_label = Utils.tsPrefixed("project_entity_label");
        public final String project_entity_with_label_config = Utils.tsPrefixed("project_entity_with_label_config");
    }

    public static class EntityLabelsAggregatorSupplier implements TransformerSupplier<
            ProjectEntityKey, EntityLabelSlotWithStringValue,
            KeyValue<ProjectEntityKey, ProjectEntityLabelValue>> {

        private final String stateStoreName;
        private final ConfluentAvroSerdes avroSerdes = new ConfluentAvroSerdes();

        EntityLabelsAggregatorSupplier(String stateStoreName) {
            this.stateStoreName = stateStoreName;
        }

        @Override
        public Transformer<ProjectEntityKey, EntityLabelSlotWithStringValue, KeyValue<ProjectEntityKey, ProjectEntityLabelValue>> get() {
            return new EntityLabelsAggregator(stateStoreName);
        }

        @Override
        public Set<StoreBuilder<?>> stores() {
            StoreBuilder<KeyValueStore<ProjectEntityKey, ProjectEntityLabelValue>> keyValueStoreBuilder =
                    Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(stateStoreName),
                            avroSerdes.ProjectEntityKey(),
                            avroSerdes.ProjectEntityLabelValue());
            return Collections.singleton(keyValueStoreBuilder);
        }
    }

    public static class EntityLabelsAggregator implements Transformer<
            ProjectEntityKey, EntityLabelSlotWithStringValue,
            KeyValue<ProjectEntityKey, ProjectEntityLabelValue>> {

        private final String stateStoreName;
        private KeyValueStore<ProjectEntityKey, ProjectEntityLabelValue> kvStore;

        public EntityLabelsAggregator(String stateStoreName) {
            this.stateStoreName = stateStoreName;
        }

        @Override
        public void init(ProcessorContext context) {
            this.kvStore = context.getStateStore(stateStoreName);
        }

        @Override
        public KeyValue<ProjectEntityKey, ProjectEntityLabelValue> transform(
                ProjectEntityKey key,
                EntityLabelSlotWithStringValue value
        ) {


            var groupKey = ProjectEntityKey.newBuilder()
                    .setEntityId(key.getEntityId())
                    .setProjectId(key.getProjectId())
                    .build();

            // get previous entity label value
            var oldVal = kvStore.get(groupKey);

            // if no slot value
            if (value == null) {

                // initialize label value
                if (oldVal == null) {

                    ArrayList<String> stringList = new ArrayList<>();
                    for (int i = 0; i < NUMBER_OF_SLOTS; i++) {
                        stringList.add("");
                    }

                    oldVal = ProjectEntityLabelValue.newBuilder()
                            .setProjectId(key.getProjectId())
                            .setEntityId(key.getEntityId())
                            .setLabel("")
                            .setLabelSlots(stringList)
                            .build();

                    kvStore.put(groupKey, oldVal);
                }
                // return old value
                return KeyValue.pair(groupKey, oldVal);
            }

            var slotNum = value.getOrdNum();

            // if no oldVal, initialize one
            if (oldVal == null) {
                ArrayList<String> stringList = new ArrayList<>();
                for (int i = 0; i < NUMBER_OF_SLOTS; i++) {
                    if (i == slotNum && !value.getDeleted$1()) stringList.add(value.getString());
                    else stringList.add("");
                }

                var initialVal = ProjectEntityLabelValue.newBuilder()
                        .setProjectId(key.getProjectId())
                        .setEntityId(key.getEntityId())
                        .setLabel(Utils.shorten(value.getString(), MAX_STRING_LENGTH))
                        .setLabelSlots(stringList)
                        .build();


                kvStore.put(groupKey, initialVal);
                return KeyValue.pair(groupKey, initialVal);
            }

            // get old slots
            var slots = oldVal.getLabelSlots();

            // add new string to slot
            if (!value.getDeleted$1()) slots.set(slotNum, value.getString());
            else slots.set(slotNum, "");

            // create new entity label
            var strings = slots.stream().filter(s -> !Objects.equals(s, "")).toList();
            var entityLabel = Utils.shorten(String.join(", ", strings), MAX_STRING_LENGTH);

            // memorize if label has changed
            var labelIsNew = !Objects.equals(entityLabel, oldVal.getLabel());

            // update the old value
            oldVal.setLabelSlots(slots);
            oldVal.setLabel(entityLabel);
            kvStore.put(groupKey, oldVal);

            // if the new entity label differs from the old, flush the new entity label value
            if (labelIsNew) {
                return KeyValue.pair(groupKey, oldVal);
            }

            return null;
        }

        public void close() {

        }

    }
}
