package org.geovistory.toolbox.streams.entity.label.processsors.project;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.entity.label.AvroSerdes;
import org.geovistory.toolbox.streams.entity.label.OutputTopicNames;
import org.geovistory.toolbox.streams.entity.label.RegisterInnerTopic;
import org.geovistory.toolbox.streams.entity.label.RegisterInputTopic;
import org.geovistory.toolbox.streams.lib.Utils;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.*;


@ApplicationScoped
public class ProjectEntityLabel {

    public static final int NUMBER_OF_SLOTS = 10;
    public static final int MAX_STRING_LENGTH = 100;


    @Inject
    AvroSerdes avroSerdes;

    @Inject
    RegisterInputTopic registerInputTopic;
    @Inject
    RegisterInnerTopic registerInnerTopic;

    @Inject
    OutputTopicNames outputTopicNames;

    public ProjectEntityLabel(AvroSerdes avroSerdes, RegisterInputTopic registerInputTopic, RegisterInnerTopic registerInnerTopic, OutputTopicNames outputTopicNames) {
        this.avroSerdes = avroSerdes;
        this.registerInputTopic = registerInputTopic;
        this.registerInnerTopic = registerInnerTopic;
        this.outputTopicNames = outputTopicNames;
    }

    public void addProcessorsStandalone() {

        addProcessors(
                registerInnerTopic.projectEntityTable(),
                registerInputTopic.projectEntityLabelConfigTable(),
                registerInnerTopic.projectTopStatementsTable()
        );
    }

    public ProjectEntityLabelReturnValue addProcessors(
            KTable<ProjectEntityKey, ProjectEntityValue> projectEntityTable,
            KTable<ProjectClassKey, ProjectEntityLabelConfigValue> projectLabelConfigTable,
            KTable<ProjectTopStatementsKey, ProjectTopStatementsValue> projectTopStatementsTable) {



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
        var projectEntityWithConfigReduced = projectEntityWithConfigStream.groupByKey().reduce((aggValue, newValue) -> {
                    var o = getLabelParts(aggValue);
                    var n = getLabelParts(newValue);
                    var diff = o.size() - n.size();
                    if (diff > 0) {
                        o.sort(Comparator.comparingInt(EntityLabelConfigPart::getOrdNum));
                        for (int i = diff; i < o.size(); i++) {
                            var deletedPart = o.get(i);
                            deletedPart.setDeleted(true);
                            n.add(deletedPart);
                        }
                    }
                    return newValue;
                },
                Named.as(inner.TOPICS.project_entity_with_label_config + "-reducer"),
                Materialized.<ProjectEntityKey, ProjectEntityWithConfigValue, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.project_entity_with_label_config + "-reduced")
                        .withKeySerde(avroSerdes.ProjectEntityKey())
                        .withValueSerde(avroSerdes.ProjectEntityWithConfigValue()
                        )
        );
        var projectEntityWithConfigReducedStream = projectEntityWithConfigReduced
                .toStream(
                        Named.as(inner.TOPICS.project_entity_with_label_config + "reduced-to-stream")
                );
        var projectEntityLabelSlots = projectEntityWithConfigReducedStream.flatMap(
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
                            var part = labelParts.get(i);
                            v = ProjectEntityLabelPartValue.newBuilder()
                                    .setEntityId(key.getEntityId())
                                    .setProjectId(key.getProjectId())
                                    .setOrdNum(i)
                                    .setConfiguration(part.getField())
                                    .setDeleted$1(part.getDeleted())
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
                    result.setDeleted$1(entityLabelSlot.getDeleted$1());

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
                .transform(new EntityLabelsAggregatorSupplier("project_entity_labels_agg", avroSerdes));

        /* SINK PROCESSORS */

        aggregatedStream.to(outputTopicNames.projectEntityLabel(),
                Produced.with(avroSerdes.ProjectEntityKey(), avroSerdes.ProjectEntityLabelValue())
                        .withName(outputTopicNames.projectEntityLabel() + "-producer")
        );

        projectEntityWithConfigReducedStream
                .mapValues((readOnlyKey, value) -> value.getLabelConfig())
                .to(outputTopicNames.projectEntityWithLabelConfig(),
                        Produced.with(avroSerdes.ProjectEntityKey(), avroSerdes.ProjectEntityLabelConfigValue())
                                .withName(outputTopicNames.projectEntityWithLabelConfig() + "-producer")
                );

        return new ProjectEntityLabelReturnValue(aggregatedStream);

    }


    private List<EntityLabelConfigPart> getLabelParts(ProjectEntityWithConfigValue in) {
        if (in.getLabelConfig() == null) return List.of();
        if (in.getLabelConfig().getConfig() == null) return List.of();
        if (in.getLabelConfig().getConfig().getLabelParts() == null) return List.of();
        return in.getLabelConfig().getConfig().getLabelParts();
    }

    public enum inner {
        TOPICS;
        public final String project_entity_with_label_config = "project_entity_with_label_config";
        public final String project_entity_label_slots = "project_entity_label_slots";
        public final String project_entity_label_slots_with_strings = "project_entity_label_slots_with_strings";
        public final String project_entity_label_slots_with_strings_repart = "project_entity_label_slots_with_strings_repart";

    }

    public static class EntityLabelsAggregatorSupplier implements TransformerSupplier<
            ProjectEntityKey, EntityLabelSlotWithStringValue,
            KeyValue<ProjectEntityKey, ProjectEntityLabelValue>> {

        private final String stateStoreName;
        private final AvroSerdes avroSerdes;

        public EntityLabelsAggregatorSupplier(String stateStoreName, AvroSerdes avroSerdes) {
            this.stateStoreName = stateStoreName;
            this.avroSerdes = avroSerdes;
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
