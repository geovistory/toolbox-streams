package org.geovistory.toolbox.streams.entity.preview.processors.project;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.entity.preview.ConfiguredAvroSerde;
import org.geovistory.toolbox.streams.entity.preview.InputTopicNames;
import org.geovistory.toolbox.streams.entity.preview.Klass;
import org.geovistory.toolbox.streams.entity.preview.OutputTopicNames;
import org.geovistory.toolbox.streams.lib.Utils;

import java.util.HashMap;
import java.util.Map;

import static org.geovistory.toolbox.streams.lib.JoinHelper.addStateStore;
import static org.geovistory.toolbox.streams.lib.JoinHelper.registerSourceTopics;


@ApplicationScoped
public class ProjectEntityPreview {


    @Inject
    ConfiguredAvroSerde avroSerdes;

    @Inject
    InputTopicNames inputTopicNames;


    @Inject
    OutputTopicNames outputTopicNames;


    public void addProcessors(
            Topology topology
    ) {

        var joinProcessorName = "create-project-entity-preview";

        registerSourceTopics(
                topology,
                inputTopicNames.getProjectEntity(),
                avroSerdes.key(),
                avroSerdes.value()
        );

        registerSourceTopics(
                topology,
                inputTopicNames.getProjectEntityLabel(),
                avroSerdes.key(),
                avroSerdes.value()
        );


        registerSourceTopics(
                topology,
                inputTopicNames.getProjectEntityType(),
                avroSerdes.key(),
                avroSerdes.value()
        );


        registerSourceTopics(
                topology,
                inputTopicNames.getProjectEntityTimeSpan(),
                avroSerdes.key(),
                avroSerdes.value()
        );


        registerSourceTopics(
                topology,
                inputTopicNames.getProjectEntityFulltext(),
                avroSerdes.key(),
                avroSerdes.value()
        );


        registerSourceTopics(
                topology,
                inputTopicNames.getProjectEntityClassLabel(),
                avroSerdes.key(),
                avroSerdes.value()
        );


        registerSourceTopics(
                topology,
                inputTopicNames.getProjectEntityClassMetadata(),
                avroSerdes.key(),
                avroSerdes.value()
        );


        // create a state store builder
        var stateStoreName = "joinedProjectPreviewStore";
        Map<String, String> changelogConfig = new HashMap<>();
        StoreBuilder<KeyValueStore<ProjectEntityKey, EntityPreviewValue>> storeSupplier =
                Stores.keyValueStoreBuilder(
                                Stores.persistentKeyValueStore(stateStoreName),
                                avroSerdes.<ProjectEntityKey>key(),
                                avroSerdes.<EntityPreviewValue>value()
                        )
                        .withLoggingEnabled(changelogConfig);


        // add join processor
        topology.addProcessor(joinProcessorName,
                () -> new JoinProcessor(stateStoreName),
                inputTopicNames.getProjectEntity() + "-process",
                inputTopicNames.getProjectEntityLabel() + "-process",
                inputTopicNames.getProjectEntityType() + "-process",
                inputTopicNames.getProjectEntityTimeSpan() + "-process",
                inputTopicNames.getProjectEntityFulltext() + "-process",
                inputTopicNames.getProjectEntityClassLabel() + "-process",
                inputTopicNames.getProjectEntityClassMetadata() + "-process"
        );

        topology.addStateStore(storeSupplier, joinProcessorName);


        addStateStore(
                topology,
                inputTopicNames.getProjectEntity(),
                avroSerdes.key(),
                avroSerdes.value(),
                joinProcessorName
        );

        addStateStore(
                topology,
                inputTopicNames.getProjectEntityLabel(),
                avroSerdes.key(),
                avroSerdes.value(),
                joinProcessorName
        );


        addStateStore(
                topology,
                inputTopicNames.getProjectEntityType(),
                avroSerdes.key(),
                avroSerdes.value(),
                joinProcessorName
        );


        addStateStore(
                topology,
                inputTopicNames.getProjectEntityTimeSpan(),
                avroSerdes.key(),
                avroSerdes.value(),
                joinProcessorName
        );


        addStateStore(
                topology,
                inputTopicNames.getProjectEntityFulltext(),
                avroSerdes.key(),
                avroSerdes.value(),
                joinProcessorName
        );


        addStateStore(
                topology,
                inputTopicNames.getProjectEntityClassLabel(),
                avroSerdes.key(),
                avroSerdes.value(),
                joinProcessorName
        );


        addStateStore(
                topology,
                inputTopicNames.getProjectEntityClassMetadata(),
                avroSerdes.key(),
                avroSerdes.value(),
                joinProcessorName
        );

        // add sink processor
        topology.addSink(
                outputTopicNames.projectEntityPreview() + "-sink",
                outputTopicNames.projectEntityPreview(),
                avroSerdes.kS(),
                avroSerdes.vS(),
                joinProcessorName
        );


    }


    private class JoinProcessor implements Processor<ProjectEntityKey, Object, ProjectEntityKey, EntityPreviewValue> {
        String stateStoreName;
        private KeyValueStore<ProjectEntityKey, EntityPreviewValue> joinedKeyValueStore;

        private KeyValueStore<ProjectEntityKey, ProjectEntityValue> projectEntityStore;
        private KeyValueStore<ProjectEntityKey, ProjectEntityLabelValue> projectEntityLabelStore;
        private KeyValueStore<ProjectEntityKey, ProjectEntityTypeValue> projectEntityTypeStore;
        private KeyValueStore<ProjectEntityKey, TimeSpanValue> projectEntityTimeSpanStore;
        private KeyValueStore<ProjectEntityKey, ProjectEntityFulltextValue> projectEntityFulltextStore;
        private KeyValueStore<ProjectEntityKey, ProjectEntityClassLabelValue> projectEntityClassLabelStore;
        private KeyValueStore<ProjectEntityKey, ProjectEntityClassMetadataValue> projectEntityClassMetadataStore;
        private ProcessorContext<ProjectEntityKey, EntityPreviewValue> context;

        public JoinProcessor(String stateStoreName) {
            this.stateStoreName = stateStoreName;
        }


        @Override
        public void process(Record<ProjectEntityKey, Object> record) {

            var projectEntity = this.projectEntityStore.get(record.key());
            if (projectEntity == null || Utils.booleanIsEqualTrue(projectEntity.getDeleted$1())) {

                // forward a tombstone
                context.forward(record.withValue(null));

            } else {
                var projectEntityLabel = this.projectEntityLabelStore.get(record.key());
                var projectEntityType = this.projectEntityTypeStore.get(record.key());
                var projectEntityTimeSpan = this.projectEntityTimeSpanStore.get(record.key());
                var projectEntityFulltext = this.projectEntityFulltextStore.get(record.key());
                var projectEntityClassLabel = this.projectEntityClassLabelStore.get(record.key());
                var projectEntityClassMetadata = this.projectEntityClassMetadataStore.get(record.key());

                var newVal = EntityPreviewValue.newBuilder()
                        .setFkProject(projectEntity.getProjectId())
                        .setProject(projectEntity.getProjectId())
                        .setEntityId(projectEntity.getEntityId())
                        .setPkEntity(parseStringId(projectEntity.getEntityId()))
                        .setFkClass(projectEntity.getClassId())
                        .setParentClasses("[]")
                        .setAncestorClasses("[]")
                        .setEntityType("")
                        .build();

                if (projectEntityLabel != null) newVal.setEntityLabel(projectEntityLabel.getLabel());

                if (projectEntityType != null) {
                    var typeId = projectEntityType.getTypeId();
                    if (typeId != null && typeId.length() > 1) {
                        newVal.setTypeId(typeId);
                        newVal.setFkType(parseStringId(typeId));
                    }
                    newVal.setTypeLabel(projectEntityType.getTypeLabel());
                }

                if (projectEntityTimeSpan != null) {
                    newVal.setTimeSpan(projectEntityTimeSpan.getTimeSpan().toString());
                    newVal.setFirstSecond(projectEntityTimeSpan.getFirstSecond());
                    newVal.setLastSecond(projectEntityTimeSpan.getLastSecond());
                }

                if (projectEntityFulltext != null) {
                    newVal.setFullText(projectEntityFulltext.getFulltext());
                }

                if (projectEntityClassLabel != null) {
                    newVal.setClassLabel(projectEntityClassLabel.getClassLabel());
                }

                if (projectEntityClassMetadata != null) {
                    var parents = projectEntityClassMetadata.getParentClasses();
                    var ancestors = projectEntityClassMetadata.getAncestorClasses();
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
        public void init(ProcessorContext<ProjectEntityKey, EntityPreviewValue> context) {

            this.joinedKeyValueStore = context.getStateStore(stateStoreName);

            this.projectEntityStore = context.getStateStore(inputTopicNames.getProjectEntity() + "-store");

            this.projectEntityLabelStore = context.getStateStore(inputTopicNames.getProjectEntityLabel() + "-store");

            this.projectEntityTypeStore = context.getStateStore(inputTopicNames.getProjectEntityType() + "-store");

            this.projectEntityTimeSpanStore = context.getStateStore(inputTopicNames.getProjectEntityTimeSpan() + "-store");

            this.projectEntityFulltextStore = context.getStateStore(inputTopicNames.getProjectEntityFulltext() + "-store");

            this.projectEntityClassLabelStore = context.getStateStore(inputTopicNames.getProjectEntityClassLabel() + "-store");

            this.projectEntityClassMetadataStore = context.getStateStore(inputTopicNames.getProjectEntityClassMetadata() + "-store");

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





