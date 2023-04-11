package org.geovistory.toolbox.streams.entity.label.processsors.community;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
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
import java.util.Collections;
import java.util.Set;


@ApplicationScoped
public class CommunityToolboxEntity {


    @Inject
    AvroSerdes avroSerdes;

    @Inject
    RegisterInputTopic registerInputTopic;
    @Inject
    RegisterInnerTopic registerInnerTopic;

    @Inject
    OutputTopicNames outputTopicNames;

    public CommunityToolboxEntity(AvroSerdes avroSerdes, RegisterInputTopic registerInputTopic, RegisterInnerTopic registerInnerTopic, OutputTopicNames outputTopicNames) {
        this.avroSerdes = avroSerdes;
        this.registerInputTopic = registerInputTopic;
        this.registerInnerTopic = registerInnerTopic;
        this.outputTopicNames = outputTopicNames;
    }

    public void addProcessorsStandalone() {

        addProcessors(
                registerInnerTopic.projectEntityVisibilityStream()
        );
    }

    public CommunityToolboxEntityReturnValue addProcessors(
            KStream<ProjectEntityKey, ProjectEntityVisibilityValue> projectEntityStream) {

        var result = projectEntityStream
                .transform(new CounterSupplier("community_entity_counter", avroSerdes));
        result.to(outputTopicNames.communityToolboxEntity(),
                Produced.with(avroSerdes.CommunityEntityKey(), avroSerdes.CommunityEntityValue())
                        .withName(outputTopicNames.communityToolboxEntity() + "-producer")
        );

        return new CommunityToolboxEntityReturnValue(result);

    }


    public enum inner {
        TOPICS
    }


    public static class CounterSupplier implements TransformerSupplier<
            ProjectEntityKey, ProjectEntityVisibilityValue,
            KeyValue<CommunityEntityKey, CommunityEntityValue>> {

        private final String stateStoreName;
        private final AvroSerdes avroSerdes;

        public CounterSupplier(String stateStoreName, AvroSerdes avroSerdes) {
            this.stateStoreName = stateStoreName;
            this.avroSerdes = avroSerdes;
        }

        @Override
        public Transformer<ProjectEntityKey, ProjectEntityVisibilityValue, KeyValue<CommunityEntityKey, CommunityEntityValue>> get() {
            return new Counter(stateStoreName);
        }

        @Override
        public Set<StoreBuilder<?>> stores() {
            StoreBuilder<KeyValueStore<CommunityEntityKey, BooleanMap>> keyValueStoreBuilder =
                    Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(stateStoreName),
                            avroSerdes.CommunityEntityKey(),
                            avroSerdes.BooleanMapValue());
            return Collections.singleton(keyValueStoreBuilder);
        }
    }

    public static class Counter implements Transformer<
            ProjectEntityKey, ProjectEntityVisibilityValue,
            KeyValue<CommunityEntityKey, CommunityEntityValue>> {

        private final String stateStoreName;
        private KeyValueStore<CommunityEntityKey, BooleanMap> kvStore;

        public Counter(String stateStoreName) {
            this.stateStoreName = stateStoreName;
        }

        @Override
        public void init(ProcessorContext context) {
            this.kvStore = context.getStateStore(stateStoreName);
        }

        @Override
        public KeyValue<CommunityEntityKey, CommunityEntityValue> transform(
                ProjectEntityKey key,
                ProjectEntityVisibilityValue projectEntityValue
        ) {
            // if we have no value, we cant do anything
            if (projectEntityValue == null) return null;

            // create CommunityEntityKey
            var k = CommunityEntityKey.newBuilder().setEntityId(key.getEntityId()).build();

            // try to get existing count map
            var existingCountMap = kvStore.get(k);

            // take existing or initialize new count map
            var countMap =
                    // if no existing count map
                    existingCountMap == null ?
                            // initialize new count map
                            BooleanMap.newBuilder().build() :
                            // else take existing count map
                            existingCountMap;

            if (
                // if input record was not deleted
                    Utils.booleanIsNotEqualTrue(projectEntityValue.getDeleted$1())
                            // and is visible for toolbox community
                            && projectEntityValue.getCommunityVisibilityToolbox()
            ) {
                // add project id to the count map
                countMap.getItem().put("" + projectEntityValue.getProjectId(), true);
            }
            // else (if input record was deleted or it is not visible for toolbox community)
            else {
                // remove the project id from count map
                countMap.getItem().remove("" + projectEntityValue.getProjectId());
            }

            // put count map to store
            kvStore.put(k, countMap);

            // create CommunityEntityValue
            var v = CommunityEntityValue.newBuilder()
                    .setClassId(projectEntityValue.getClassId())
                    .setEntityId(key.getEntityId())
                    .setProjectCount(countMap.getItem().size())
                    .build();

            // return it (down stream)
            return KeyValue.pair(k, v);

        }

        public void close() {

        }

    }

}
