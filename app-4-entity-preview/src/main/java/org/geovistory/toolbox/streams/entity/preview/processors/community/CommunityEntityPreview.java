package org.geovistory.toolbox.streams.entity.preview.processors.community;

import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.entity.preview.AvroSerdes;
import org.geovistory.toolbox.streams.entity.preview.InputTopicNames;
import org.geovistory.toolbox.streams.entity.preview.Klass;
import org.geovistory.toolbox.streams.entity.preview.OutputTopicNames;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;

import static org.geovistory.toolbox.streams.lib.JoinHelper.addStateStore;
import static org.geovistory.toolbox.streams.lib.JoinHelper.registerSourceTopics;


@ApplicationScoped
public class CommunityEntityPreview {


    @Inject
    AvroSerdes avroSerdes;

    @Inject
    InputTopicNames inputTopicNames;


    @Inject
    OutputTopicNames outputTopicNames;


    public CommunityEntityPreview(AvroSerdes avroSerdes, InputTopicNames inputTopicNames, OutputTopicNames outputTopicNames) {
        this.avroSerdes = avroSerdes;
        this.inputTopicNames = inputTopicNames;
        this.outputTopicNames = outputTopicNames;
    }

    public void addProcessors(
            Topology topology
    ) {

        var joinProcessorName = "create-community-entity-preview";

        registerSourceTopics(
                topology,
                inputTopicNames.getCommunityEntity(),
                avroSerdes.CommunityEntityKey(),
                avroSerdes.CommunityEntityValue()
        );

        registerSourceTopics(
                topology,
                inputTopicNames.getCommunityEntityLabel(),
                avroSerdes.CommunityEntityKey(),
                avroSerdes.CommunityEntityLabelValue()
        );

        registerSourceTopics(
                topology,
                inputTopicNames.getCommunityEntityType(),
                avroSerdes.CommunityEntityKey(),
                avroSerdes.CommunityEntityTypeValue()
        );


        registerSourceTopics(
                topology,
                inputTopicNames.getCommunityEntityTimeSpan(),
                avroSerdes.CommunityEntityKey(),
                avroSerdes.TimeSpanValue()
        );


        registerSourceTopics(
                topology,
                inputTopicNames.getCommunityEntityFulltext(),
                avroSerdes.CommunityEntityKey(),
                avroSerdes.CommunityEntityFulltextValue()
        );


        registerSourceTopics(
                topology,
                inputTopicNames.getCommunityEntityClassLabel(),
                avroSerdes.CommunityEntityKey(),
                avroSerdes.CommunityEntityClassLabelValue()
        );


        registerSourceTopics(
                topology,
                inputTopicNames.getCommunityEntityClassMetadata(),
                avroSerdes.CommunityEntityKey(),
                avroSerdes.CommunityEntityClassMetadataValue()
        );


        // create a state store builder
        var stateStoreName = "joinedStore";
        Map<String, String> changelogConfig = new HashMap<>();
        StoreBuilder<KeyValueStore<CommunityEntityKey, EntityPreviewValue>> storeSupplier =
                Stores.keyValueStoreBuilder(
                                Stores.persistentKeyValueStore(stateStoreName),
                                avroSerdes.CommunityEntityKey(),
                                avroSerdes.EntityPreviewValue())
                        .withLoggingEnabled(changelogConfig);


        // add join processor
        topology.addProcessor(joinProcessorName,
                () -> new JoinProcessor(stateStoreName),
                inputTopicNames.communityEntity + "-process",
                inputTopicNames.getCommunityEntityLabel() + "-process",
                inputTopicNames.getCommunityEntityType() + "-process",
                inputTopicNames.getCommunityEntityTimeSpan() + "-process",
                inputTopicNames.getCommunityEntityFulltext() + "-process",
                inputTopicNames.getCommunityEntityClassLabel() + "-process",
                inputTopicNames.getCommunityEntityClassMetadata() + "-process"
        );

        topology.addStateStore(storeSupplier, joinProcessorName);


        addStateStore(
                topology,
                inputTopicNames.getCommunityEntity(),
                avroSerdes.CommunityEntityKey(),
                avroSerdes.CommunityEntityValue(),
                joinProcessorName
        );

        addStateStore(
                topology,
                inputTopicNames.getCommunityEntityLabel(),
                avroSerdes.CommunityEntityKey(),
                avroSerdes.CommunityEntityLabelValue(),
                joinProcessorName
        );


        addStateStore(
                topology,
                inputTopicNames.getCommunityEntityType(),
                avroSerdes.CommunityEntityKey(),
                avroSerdes.CommunityEntityTypeValue(),
                joinProcessorName
        );


        addStateStore(
                topology,
                inputTopicNames.getCommunityEntityTimeSpan(),
                avroSerdes.CommunityEntityKey(),
                avroSerdes.TimeSpanValue(),
                joinProcessorName
        );


        addStateStore(
                topology,
                inputTopicNames.getCommunityEntityFulltext(),
                avroSerdes.CommunityEntityKey(),
                avroSerdes.CommunityEntityFulltextValue(),
                joinProcessorName
        );


        addStateStore(
                topology,
                inputTopicNames.getCommunityEntityClassLabel(),
                avroSerdes.CommunityEntityKey(),
                avroSerdes.CommunityEntityClassLabelValue(),
                joinProcessorName
        );


        addStateStore(
                topology,
                inputTopicNames.getCommunityEntityClassMetadata(),
                avroSerdes.CommunityEntityKey(),
                avroSerdes.CommunityEntityClassMetadataValue(),
                joinProcessorName
        );

        // add sink processor
        topology.addSink(
                outputTopicNames.communityEntityPreview() + "-sink",
                outputTopicNames.communityEntityPreview(),
                avroSerdes.CommunityEntityKey().serializer(),
                avroSerdes.EntityPreviewValue().serializer(),
                joinProcessorName
        );


    }

    private class JoinProcessor implements Processor<CommunityEntityKey, Object, CommunityEntityKey, EntityPreviewValue> {
        String stateStoreName;
        private KeyValueStore<CommunityEntityKey, EntityPreviewValue> joinedKeyValueStore;

        private KeyValueStore<CommunityEntityKey, CommunityEntityValue> communityEntityStore;
        private KeyValueStore<CommunityEntityKey, CommunityEntityLabelValue> communityEntityLabelStore;
        private KeyValueStore<CommunityEntityKey, CommunityEntityTypeValue> communityEntityTypeStore;
        private KeyValueStore<CommunityEntityKey, TimeSpanValue> communityEntityTimeSpanStore;
        private KeyValueStore<CommunityEntityKey, CommunityEntityFulltextValue> communityEntityFulltextStore;
        private KeyValueStore<CommunityEntityKey, CommunityEntityClassLabelValue> communityEntityClassLabelStore;
        private KeyValueStore<CommunityEntityKey, CommunityEntityClassMetadataValue> communityEntityClassMetadataStore;
        private ProcessorContext<CommunityEntityKey, EntityPreviewValue> context;

        public JoinProcessor(String stateStoreName) {
            this.stateStoreName = stateStoreName;
        }


        @Override
        public void process(Record<CommunityEntityKey, Object> record) {

            var communityEntity = this.communityEntityStore.get(record.key());
            if (
                    communityEntity == null ||
                            // hide entities with 0 projects
                            communityEntity.getProjectCount() < 1
            ) {

                // forward a tombstone
                context.forward(record.withValue(null));

            } else {
                var communityEntityLabel = this.communityEntityLabelStore.get(record.key());
                var communityEntityType = this.communityEntityTypeStore.get(record.key());
                var communityEntityTimeSpan = this.communityEntityTimeSpanStore.get(record.key());
                var communityEntityFulltext = this.communityEntityFulltextStore.get(record.key());
                var communityEntityClassLabel = this.communityEntityClassLabelStore.get(record.key());
                var communityEntityClassMetadata = this.communityEntityClassMetadataStore.get(record.key());

                var newVal = EntityPreviewValue.newBuilder()
                        .setFkProject(null)
                        .setProject(0)
                        .setEntityId(communityEntity.getEntityId())
                        .setPkEntity(parseStringId(communityEntity.getEntityId()))
                        .setFkClass(communityEntity.getClassId())
                        .setParentClasses("[]")
                        .setAncestorClasses("[]")
                        .setEntityType("")
                        .build();

                if (communityEntityLabel != null) newVal.setEntityLabel(communityEntityLabel.getLabel());

                if (communityEntityType != null) {
                    var typeId = communityEntityType.getTypeId();
                    if (typeId != null && typeId.length() > 1) {
                        newVal.setTypeId(typeId);
                        newVal.setFkType(parseStringId(typeId));
                    }
                    newVal.setTypeLabel(communityEntityType.getTypeLabel());
                }

                if (communityEntityTimeSpan != null) {
                    newVal.setTimeSpan(communityEntityTimeSpan.getTimeSpan().toString());
                    newVal.setFirstSecond(communityEntityTimeSpan.getFirstSecond());
                    newVal.setLastSecond(communityEntityTimeSpan.getLastSecond());
                }

                if (communityEntityFulltext != null) {
                    newVal.setFullText(communityEntityFulltext.getFulltext());
                }

                if (communityEntityClassLabel != null) {
                    newVal.setClassLabel(communityEntityClassLabel.getClassLabel());
                }

                if (communityEntityClassMetadata != null) {
                    var parents = communityEntityClassMetadata.getParentClasses();
                    var ancestors = communityEntityClassMetadata.getAncestorClasses();
                    newVal.setParentClasses(parents.toString());
                    newVal.setAncestorClasses(ancestors.toString());
                    var isPersistentItem = parents.contains(Klass.PERSISTENT_ITEM.get()) ||
                            ancestors.contains(Klass.PERSISTENT_ITEM.get());
                    var entityType = isPersistentItem ? "peIt" : "teEn";
                    newVal.setEntityType(entityType);
                }

                // Put into state store.
                joinedKeyValueStore.put(record.key(), newVal);

                // forward join result
                context.forward(record.withValue(newVal));

            }
        }

        @Override
        public void init(ProcessorContext<CommunityEntityKey, EntityPreviewValue> context) {

            this.joinedKeyValueStore = context.getStateStore(stateStoreName);

            this.communityEntityStore = context.getStateStore(inputTopicNames.getCommunityEntity() + "-store");

            this.communityEntityLabelStore = context.getStateStore(inputTopicNames.getCommunityEntityLabel() + "-store");

            this.communityEntityTypeStore = context.getStateStore(inputTopicNames.getCommunityEntityType() + "-store");

            this.communityEntityTimeSpanStore = context.getStateStore(inputTopicNames.getCommunityEntityTimeSpan() + "-store");

            this.communityEntityFulltextStore = context.getStateStore(inputTopicNames.getCommunityEntityFulltext() + "-store");

            this.communityEntityClassLabelStore = context.getStateStore(inputTopicNames.getCommunityEntityClassLabel() + "-store");

            this.communityEntityClassMetadataStore = context.getStateStore(inputTopicNames.getCommunityEntityClassMetadata() + "-store");

            this.context = context;
        }

        @Override
        public void close() {
        }

        private static int parseStringId(String value1) {
            try {
                return Integer.parseInt(value1.substring(1));
            } catch (NumberFormatException | IndexOutOfBoundsException e) {
                e.printStackTrace();
            }
            return 0;
        }
    }


}





