package org.geovistory.toolbox.streams.entity.processors.community;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.entity.RegisterInnerTopic;
import org.geovistory.toolbox.streams.entity.RegisterInputTopic;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.Utils;

import java.util.ArrayList;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.stream.Stream;


public class CommunityEntityFulltext {

    public static void main(String[] args) {
        System.out.println(buildStandalone(new StreamsBuilder(), "toolbox").describe());
    }

    public static Topology buildStandalone(StreamsBuilder builder, String nameSupplement) {
        var innerTopic = new RegisterInnerTopic(builder);
        var inputTopic = new RegisterInputTopic(builder);

        return addProcessors(
                builder,
                innerTopic.communityEntityTopStatementsTable(nameSupplement),
                inputTopic.communityEntityLabelConfigTable(),
                nameSupplement
        ).builder().build();
    }

    public static CommunityEntityFulltextReturnValue addProcessors(
            StreamsBuilder builder,
            KTable<CommunityEntityKey, CommunityEntityTopStatementsValue> communityEntityTopStatementsTable,
            KTable<CommunityEntityLabelConfigKey, CommunityEntityLabelConfigValue> communityLabelConfigTable,
            String nameSupplement
    ) {

        var avroSerdes = new ConfluentAvroSerdes();

        var n = "communtiy_" + nameSupplement + "_entity_top_statements_with_label_config";

        /* STREAM PROCESSORS */
        // 2)

        var communityEntityWithConfigTable = communityEntityTopStatementsTable.leftJoin(
                communityLabelConfigTable,
                communityEntityValue -> CommunityEntityLabelConfigKey.newBuilder()
                        .setClassId(communityEntityValue.getClassId())
                        .build(),
                (value1, value2) -> CommunityEntityTopStatementsWithConfigValue.newBuilder()
                        .setEntityTopStatements(value1)
                        .setLabelConfig(value2)
                        .setProjectCount(value1.getProjectCount())
                        .build(),
                TableJoined.as(n + "-fk-left-join"),
                Materialized.<CommunityEntityKey, CommunityEntityTopStatementsWithConfigValue, KeyValueStore<Bytes, byte[]>>as(n)
                        .withKeySerde(avroSerdes.CommunityEntityKey())
                        .withValueSerde(avroSerdes.CommunityEntityTopStatementsWithConfigValue())
        );

        // 3
        var communityEntityFulltextStream = communityEntityWithConfigTable.toStream(
                        Named.as(n + "-to-stream")
                )
                .mapValues(
                        (readOnlyKey, value) -> {
                            var fulltext = createFulltext(value);
                            return CommunityEntityFulltextValue.newBuilder()
                                    .setEntityId(readOnlyKey.getEntityId())
                                    .setFulltext(fulltext)
                                    .setProjectCount(value.getProjectCount()).build();
                        },
                        Named.as("kstream-mapvalues-community-entity-top-statements-with-config-value-to-community-entity-fulltext-value")
                );

        /* SINK PROCESSORS */

        communityEntityFulltextStream.to(getOutputTopicName(nameSupplement),
                Produced.with(avroSerdes.CommunityEntityKey(), avroSerdes.CommunityEntityFulltextValue())
                        .withName(getOutputTopicName(nameSupplement) + "-producer")
        );

        return new CommunityEntityFulltextReturnValue(builder, communityEntityFulltextStream);

    }


    /**
     * creates the fulltext for an entity
     *
     * @param v a CommunityEntityTopStatementsWithConfigValue
     * @return Fulltext string.
     */
    public static String createFulltext(CommunityEntityTopStatementsWithConfigValue v) {
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

    private static String getEntityLabel(Map<String, CommunityTopStatementsWithPropLabelValue> topStatementsMap) {
        try {
            var firstField = topStatementsMap.entrySet().iterator().next().getValue();
            var firstStatement = firstField.getStatements().get(0).getStatement();
            return firstField.getIsOutgoing() ?
                    firstStatement.getSubjectLabel() : firstStatement.getObjectLabel();
        } catch (NoSuchElementException e) {
            return null;
        }
    }

    private static String createFieldText(Map<String, CommunityTopStatementsWithPropLabelValue> topStatementsMap,
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

    public static String getOutputTopicName(String nameSupplement) {
        return Utils.tsPrefixed("community_" + nameSupplement + "_entity_fulltext");
    }
}
