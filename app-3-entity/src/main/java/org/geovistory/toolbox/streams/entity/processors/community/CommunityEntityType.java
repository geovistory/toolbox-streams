package org.geovistory.toolbox.streams.entity.processors.community;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.entity.ConfiguredAvroSerde;
import org.geovistory.toolbox.streams.entity.OutputTopicNames;
import org.geovistory.toolbox.streams.lib.Utils;


@ApplicationScoped
public class CommunityEntityType {


    @Inject
    ConfiguredAvroSerde avroSerdes;


    @Inject
    OutputTopicNames outputTopicNames;

    @ConfigProperty(name = "ts.community.slug", defaultValue = "")
    private String communitySlug;


    public CommunityEntityTypeReturnValue addProcessors(
            KTable<ProjectEntityKey, ProjectEntityValue> communityEntityTable,
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
                (value1, value2) -> ProjectEntityHasTypePropValue.newBuilder()
                        .setEntityId(value1.getEntityId())
                        .setProjectId(value1.getProjectId())
                        .setHasTypePropertyId(value2.getPropertyId())
                        .setDeleted$1(value2.getDeleted$1())
                        .build(),
                TableJoined.as(communityEntityWithHasTypeProperty + "-fk-join"),
                Materialized.<ProjectEntityKey, ProjectEntityHasTypePropValue, KeyValueStore<Bytes, byte[]>>as(communityEntityWithHasTypeProperty)
                        .withKeySerde(avroSerdes.<ProjectEntityKey>key())
                        .withValueSerde(avroSerdes.<ProjectEntityHasTypePropValue>value())
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
                TableJoined.as(communityEntityWithHasTypeStatement + "-fk-join"),
                Materialized.<ProjectEntityKey, ProjectEntityTypeValue, KeyValueStore<Bytes, byte[]>>as(communityEntityWithHasTypeStatement)
                        .withKeySerde(avroSerdes.<ProjectEntityKey>key())
                        .withValueSerde(avroSerdes.<ProjectEntityTypeValue>value())
        );

        var communityEntityTypeStream = communityEntityTypeTable.toStream(
                Named.as(communityEntityWithHasTypeStatement + "-to-stream")
        );
        /* SINK PROCESSORS */

        communityEntityTypeStream.to(outputTopicNames.communityEntityType(),
                Produced.with(avroSerdes.<ProjectEntityKey>key(), avroSerdes.<ProjectEntityTypeValue>value())
                        .withName(outputTopicNames.communityEntityType() + "-producer")
        );

        return new CommunityEntityTypeReturnValue( communityEntityTypeTable, communityEntityTypeStream);

    }


}
