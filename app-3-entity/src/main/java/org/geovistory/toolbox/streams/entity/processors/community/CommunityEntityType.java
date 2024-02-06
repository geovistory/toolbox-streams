package org.geovistory.toolbox.streams.entity.processors.community;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.entity.AvroSerdes;
import org.geovistory.toolbox.streams.entity.OutputTopicNames;
import org.geovistory.toolbox.streams.entity.RegisterInputTopic;
import org.geovistory.toolbox.streams.lib.Utils;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;


@ApplicationScoped
public class CommunityEntityType {


    @Inject
    AvroSerdes avroSerdes;

    @Inject
    RegisterInputTopic registerInputTopic;


    @Inject
    OutputTopicNames outputTopicNames;

    @ConfigProperty(name = "ts.community.slug", defaultValue = "")
    private String communitySlug;


    public CommunityEntityType(AvroSerdes avroSerdes, RegisterInputTopic registerInputTopic, OutputTopicNames outputTopicNames) {
        this.avroSerdes = avroSerdes;
        this.registerInputTopic = registerInputTopic;
        this.outputTopicNames = outputTopicNames;
    }

    public void addProcessorsStandalone() {
        addProcessors(
                registerInputTopic.communityEntityTable(),
                registerInputTopic.hasTypePropertyTable(),
                registerInputTopic.communityTopOutgoingStatementsTable()
        );
    }

    public CommunityEntityTypeReturnValue addProcessors(
            KTable<CommunityEntityKey, CommunityEntityValue> communityEntityTable,
            KTable<HasTypePropertyKey, HasTypePropertyValue> hasTypePropertyTable,
            KTable<CommunityTopStatementsKey, CommunityTopStatementsValue> communityTopOutgoingStatementsTable
            ) {

        String communityEntityWithHasTypeProperty = "community_" + communitySlug + "entity_with_has_type_property";
        String communityEntityWithHasTypeStatement = "community_" + communitySlug + "entity_with_has_type_statement";

        /* STREAM PROCESSORS */
        // 2)

        var communityEntityWithHasTypeProp = communityEntityTable.join(
                hasTypePropertyTable,
                communityEntityValue -> HasTypePropertyKey.newBuilder()
                        .setClassId(communityEntityValue.getClassId())
                        .build(),
                (value1, value2) -> CommunityEntityHasTypePropValue.newBuilder()
                        .setHasTypePropertyId(value2.getPropertyId())
                        .setEntityId(value1.getEntityId())
                        .setProjectCount(value1.getProjectCount())
                        .setDeleted$1(value2.getDeleted$1())
                        .build(),
                TableJoined.as(communityEntityWithHasTypeProperty + "-fk-join"),
                Materialized.<CommunityEntityKey, CommunityEntityHasTypePropValue, KeyValueStore<Bytes, byte[]>>as(communityEntityWithHasTypeProperty)
                        .withKeySerde(avroSerdes.CommunityEntityKey())
                        .withValueSerde(avroSerdes.CommunityEntityHasTypePropValue())
        );

        // 2)

        var communityEntityTypeTable = communityEntityWithHasTypeProp.join(
                communityTopOutgoingStatementsTable,
                communityEntityValue -> CommunityTopStatementsKey.newBuilder()
                        .setIsOutgoing(true)
                        .setEntityId(communityEntityValue.getEntityId())
                        .setPropertyId(communityEntityValue.getHasTypePropertyId())
                        .build(),
                (value1, value2) -> {
                    var statements = value2.getStatements();
                    var hasTypeStatement = statements.size() == 0 ? null : value2.getStatements().get(0);
                    var deleted = hasTypeStatement == null || Utils.booleanIsEqualTrue(value1.getDeleted$1());
                    var newVal = CommunityEntityTypeValue.newBuilder()
                            .setEntityId(value1.getEntityId());
                    if (deleted) {
                        return newVal
                                .setTypeId("")
                                .setTypeLabel(null)
                                .setProjectCount(value1.getProjectCount())
                                .setDeleted$1(true)
                                .build();
                    } else {
                        return newVal
                                .setTypeId(hasTypeStatement.getStatement().getObjectId())
                                .setTypeLabel(hasTypeStatement.getStatement().getObjectLabel())
                                .setProjectCount(value1.getProjectCount())
                                .setDeleted$1(false)
                                .build();
                    }

                },
                TableJoined.as(communityEntityWithHasTypeStatement + "-fk-join"),
                Materialized.<CommunityEntityKey, CommunityEntityTypeValue, KeyValueStore<Bytes, byte[]>>as(communityEntityWithHasTypeStatement)
                        .withKeySerde(avroSerdes.CommunityEntityKey())
                        .withValueSerde(avroSerdes.CommunityEntityTypeValue())
        );

        var communityEntityTypeStream = communityEntityTypeTable.toStream(
                Named.as(communityEntityWithHasTypeStatement + "-to-stream")
        );
        /* SINK PROCESSORS */

        communityEntityTypeStream.to(outputTopicNames.communityEntityType(),
                Produced.with(avroSerdes.CommunityEntityKey(), avroSerdes.CommunityEntityTypeValue())
                        .withName(outputTopicNames.communityEntityType() + "-producer")
        );

        return new CommunityEntityTypeReturnValue( communityEntityTypeTable, communityEntityTypeStream);

    }


}
