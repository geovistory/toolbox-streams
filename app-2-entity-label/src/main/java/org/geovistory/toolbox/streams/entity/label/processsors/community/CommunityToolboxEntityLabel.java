package org.geovistory.toolbox.streams.entity.label.processsors.community;

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
public class CommunityToolboxEntityLabel {

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

    public CommunityToolboxEntityLabel(AvroSerdes avroSerdes, RegisterInputTopic registerInputTopic, RegisterInnerTopic registerInnerTopic, OutputTopicNames outputTopicNames) {
        this.avroSerdes = avroSerdes;
        this.registerInputTopic = registerInputTopic;
        this.registerInnerTopic = registerInnerTopic;
        this.outputTopicNames = outputTopicNames;
    }

    public void addProcessorsStandalone() {

        addProcessors(
                registerInnerTopic.communityToolboxEntityTable(),
                registerInputTopic.communityEntityLabelConfigTable(),
                registerInnerTopic.communityToolboxTopStatementsTable()
        );

    }

    public CommunityEntityLabelReturnValue addProcessors(
            KTable<CommunityEntityKey, CommunityEntityValue> communityToolboxEntityTable,
            KTable<CommunityEntityLabelConfigKey, CommunityEntityLabelConfigValue> communityEntityLabelConfigTable,
            KTable<CommunityTopStatementsKey, CommunityTopStatementsValue> communityToolboxTopStatementsTable) {

        /* STREAM PROCESSORS */
        // 2)

        var communityEntityWithConfigTable = communityToolboxEntityTable.leftJoin(
                communityEntityLabelConfigTable,
                communityEntityValue -> CommunityEntityLabelConfigKey.newBuilder()
                        .setClassId(communityEntityValue.getClassId())
                        .build(),
                (value1, value2) -> CommunityEntityWithConfigValue.newBuilder()
                        .setEntity(value1)
                        .setLabelConfig(value2)
                        .build(),
                TableJoined.as(inner.TOPICS.community_toolbox_entity_with_label_config + "-fk-left-join"),
                Materialized.<CommunityEntityKey, CommunityEntityWithConfigValue, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.community_toolbox_entity_with_label_config)
                        .withKeySerde(avroSerdes.CommunityEntityKey())
                        .withValueSerde(avroSerdes.CommunityEntityWithConfigValue())
        );

        // 3
        var communityEntityWithConfigStream = communityEntityWithConfigTable
                .toStream(
                        Named.as(inner.TOPICS.community_toolbox_entity_with_label_config + "-to-stream")
                );
        var communityEntityWithConfigReduced = communityEntityWithConfigStream.groupByKey().reduce((aggValue, newValue) -> {
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
                Named.as(inner.TOPICS.community_toolbox_entity_with_label_config + "-reducer"),
                Materialized.<CommunityEntityKey, CommunityEntityWithConfigValue, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.community_toolbox_entity_with_label_config + "-reduced")
                        .withKeySerde(avroSerdes.CommunityEntityKey())
                        .withValueSerde(avroSerdes.CommunityEntityWithConfigValue()
                        )
        );
        var communityEntityLabelSlots = communityEntityWithConfigReduced
                .toStream(
                        Named.as(inner.TOPICS.community_toolbox_entity_with_label_config + "reduced-to-stream")
                )
                .flatMap(
                        (key, value) -> {
                            List<KeyValue<CommunityEntityLabelPartKey, CommunityEntityLabelPartValue>> result = new LinkedList<>();

                            if (value.getLabelConfig() == null) return result;
                            if (value.getLabelConfig().getConfig() == null) return result;
                            if (value.getLabelConfig().getConfig().getLabelParts() == null) return result;

                            var labelParts = value.getLabelConfig().getConfig().getLabelParts();
                            // sort parts according to ord num
                            labelParts.sort(Comparator.comparingInt(EntityLabelConfigPart::getOrdNum));

                            // create  slots, be they configured or not
                            for (int i = 0; i < NUMBER_OF_SLOTS; i++) {
                                var k = CommunityEntityLabelPartKey.newBuilder()
                                        .setEntityId(key.getEntityId())
                                        .setOrdNum(i)
                                        .build();

                                // if no config for slot, then v = null
                                CommunityEntityLabelPartValue v = null;
                                // else assign the value
                                if (labelParts.size() > i && labelParts.get(i) != null && labelParts.get(i).getField() != null) {
                                    var part = labelParts.get(i);
                                    v = CommunityEntityLabelPartValue.newBuilder()
                                            .setEntityId(key.getEntityId())
                                            .setOrdNum(i)
                                            .setConfiguration(part.getField())
                                            .setDeleted$1(part.getDeleted())
                                            .build();
                                }
                                result.add(KeyValue.pair(k, v));
                            }

                            return result;
                        },
                        Named.as("kstream-flatmap-community-toolbox-entity-with-config-to-community-toolbox-entity-label-slots")
                );

        // 4
        var communityEntityLabelSlotsTable = communityEntityLabelSlots.toTable(
                Named.as(inner.TOPICS.community_toolbox_entity_label_slots + "-to-table"),
                Materialized.<CommunityEntityLabelPartKey, CommunityEntityLabelPartValue, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.community_toolbox_entity_label_slots)
                        .withKeySerde(avroSerdes.CommunityEntityLabelPartKey())
                        .withValueSerde(avroSerdes.CommunityEntityLabelPartValue())
        );
        var communityEntityLabelSlotsWithStrings = communityEntityLabelSlotsTable.leftJoin(
                communityToolboxTopStatementsTable,
                (v) -> CommunityTopStatementsKey.newBuilder()
                        .setEntityId(v.getEntityId())
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
                            .map(communityStatementValue -> {
                                if (config.getIsOutgoing())
                                    return communityStatementValue.getStatement().getObjectLabel();
                                else return communityStatementValue.getStatement().getSubjectLabel();
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
                TableJoined.as(inner.TOPICS.community_toolbox_entity_label_slots_with_strings + "-fk-left-join"),
                Materialized.<CommunityEntityLabelPartKey, EntityLabelSlotWithStringValue, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.community_toolbox_entity_label_slots_with_strings)
                        .withKeySerde(avroSerdes.CommunityEntityLabelPartKey())
                        .withValueSerde(avroSerdes.ProjectEntityLabelSlotWithStringValue())
        );


        var aggregatedStream = communityEntityLabelSlotsWithStrings
                .toStream(
                        Named.as(inner.TOPICS.community_toolbox_entity_label_slots_with_strings + "-to-stream")
                )
                .selectKey((k, v) -> CommunityEntityKey.newBuilder()
                                .setEntityId(k.getEntityId())
                                .build(),
                        Named.as("kstream-select-key-of-community-toolbox-entity-label-slots")
                )
                .repartition(
                        Repartitioned.<CommunityEntityKey, EntityLabelSlotWithStringValue>as(inner.TOPICS.community_toolbox_entity_label_slots_with_strings_repart)
                                .withKeySerde(avroSerdes.CommunityEntityKey())
                                .withValueSerde(avroSerdes.ProjectEntityLabelSlotWithStringValue())
                                .withName(inner.TOPICS.community_toolbox_entity_label_slots_with_strings_repart)
                )
                .transform(new EntityLabelsAggregatorSupplier("community_toolbox_entity_labels_agg", avroSerdes));

        /* SINK PROCESSORS */

        aggregatedStream.to(outputTopicNames.communityToolboxEntityLabel(),
                Produced.with(avroSerdes.CommunityEntityKey(), avroSerdes.CommunityEntityLabelValue())
                        .withName(outputTopicNames.communityToolboxEntityLabel() + "-producer")
        );

        communityEntityWithConfigStream
                .mapValues((readOnlyKey, value) -> value.getLabelConfig())
                .to(outputTopicNames.communityToolboxEntityWithLabelConfig(),
                        Produced.with(avroSerdes.CommunityEntityKey(), avroSerdes.CommunityEntityLabelConfigValue())
                                .withName(outputTopicNames.communityToolboxEntityWithLabelConfig() + "-producer")
                );

        return new CommunityEntityLabelReturnValue(aggregatedStream);

    }

    private List<EntityLabelConfigPart> getLabelParts(CommunityEntityWithConfigValue in) {
        if (in.getLabelConfig() == null) return List.of();
        if (in.getLabelConfig().getConfig() == null) return List.of();
        if (in.getLabelConfig().getConfig().getLabelParts() == null) return List.of();
        return in.getLabelConfig().getConfig().getLabelParts();
    }


    public enum inner {
        TOPICS;
        public final String community_toolbox_entity_with_label_config = "community_toolbox_entity_with_label_config";
        public final String community_toolbox_entity_label_slots = "community_toolbox_entity_label_slots";
        public final String community_toolbox_entity_label_slots_with_strings = "community_toolbox_entity_label_slots_with_strings";
        public final String community_toolbox_entity_label_slots_with_strings_repart = "community_toolbox_entity_label_slots_with_strings_repart";

    }


    public static class EntityLabelsAggregatorSupplier implements TransformerSupplier<
            CommunityEntityKey, EntityLabelSlotWithStringValue,
            KeyValue<CommunityEntityKey, CommunityEntityLabelValue>> {

        private final String stateStoreName;
        private final AvroSerdes avroSerdes;

        public EntityLabelsAggregatorSupplier(String stateStoreName, AvroSerdes avroSerdes) {
            this.stateStoreName = stateStoreName;
            this.avroSerdes = avroSerdes;
        }

        @Override
        public Transformer<CommunityEntityKey, EntityLabelSlotWithStringValue, KeyValue<CommunityEntityKey, CommunityEntityLabelValue>> get() {
            return new EntityLabelsAggregator(stateStoreName);
        }

        @Override
        public Set<StoreBuilder<?>> stores() {
            StoreBuilder<KeyValueStore<CommunityEntityKey, CommunityEntityLabelValue>> keyValueStoreBuilder =
                    Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(stateStoreName),
                            avroSerdes.CommunityEntityKey(),
                            avroSerdes.CommunityEntityLabelValue());
            return Collections.singleton(keyValueStoreBuilder);
        }
    }

    public static class EntityLabelsAggregator implements Transformer<
            CommunityEntityKey, EntityLabelSlotWithStringValue,
            KeyValue<CommunityEntityKey, CommunityEntityLabelValue>> {

        private final String stateStoreName;
        private KeyValueStore<CommunityEntityKey, CommunityEntityLabelValue> kvStore;

        public EntityLabelsAggregator(String stateStoreName) {
            this.stateStoreName = stateStoreName;
        }

        @Override
        public void init(ProcessorContext context) {
            this.kvStore = context.getStateStore(stateStoreName);
        }

        @Override
        public KeyValue<CommunityEntityKey, CommunityEntityLabelValue> transform(
                CommunityEntityKey key,
                EntityLabelSlotWithStringValue value
        ) {
            var groupKey = CommunityEntityKey.newBuilder()
                    .setEntityId(key.getEntityId())
                    .build();

            // get previous entity label value
            var oldVal = kvStore.get(groupKey);

            // validate value

            // if no slot value
            if (value == null) {

                // initialize label value
                if (oldVal == null) {

                    ArrayList<String> stringList = new ArrayList<>();
                    for (int i = 0; i < NUMBER_OF_SLOTS; i++) {
                        stringList.add("");
                    }

                    oldVal = CommunityEntityLabelValue.newBuilder()
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

                var initialVal = CommunityEntityLabelValue.newBuilder()
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
