package org.geovistory.toolbox.streams.base.config.processors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.geovistory.toolbox.streams.avro.CommunityEntityLabelConfigKey;
import org.geovistory.toolbox.streams.avro.CommunityEntityLabelConfigValue;
import org.geovistory.toolbox.streams.avro.EntityLabelConfig;
import org.geovistory.toolbox.streams.base.config.*;
import org.geovistory.toolbox.streams.lib.Utils;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Collections;
import java.util.Set;


@ApplicationScoped
public class CommunityEntityLabelConfig {


    @Inject
    AvroSerdes avroSerdes;

    @Inject
    RegisterInputTopic registerInputTopic;
    @Inject
    RegisterInnerTopic registerInnerTopic;

    @Inject
    OutputTopicNames outputTopicNames;

    public CommunityEntityLabelConfig(AvroSerdes avroSerdes, RegisterInputTopic registerInputTopic, RegisterInnerTopic registerInnerTopic, OutputTopicNames outputTopicNames) {
        this.avroSerdes = avroSerdes;
        this.registerInputTopic = registerInputTopic;
        this.registerInnerTopic = registerInnerTopic;
        this.outputTopicNames = outputTopicNames;
    }

    public void addProcessorsStandalone() {
        addProcessors(
                registerInputTopic.proEntityLabelConfigStream()
        );
    }


    public CommunityEntityLabelConfigReturnValue addProcessors(
            KStream<dev.projects.entity_label_config.Key, dev.projects.entity_label_config.Value> proEntityLabelConfigStream
    ) {
        /* STREAM PROCESSORS */
        // 2)
        var communityEntityLabelConfigStream = proEntityLabelConfigStream.transform(
                new TransformSupplier("kstream-flatmap-project-entity-label-config-to-community-entity-label-config", avroSerdes)
        );
        /* SINK PROCESSORS */

        // 8) to
        communityEntityLabelConfigStream.to(outputTopicNames.communityEntityLabelConfig(),
                Produced.with(avroSerdes.CommunityEntityLabelConfigKey(), avroSerdes.CommunityEntityLabelConfigValue())
                        .withName(outputTopicNames.communityEntityLabelConfig() + "-producer")
        );

        return new CommunityEntityLabelConfigReturnValue(communityEntityLabelConfigStream);

    }


    public static class TransformSupplier implements TransformerSupplier<
            dev.projects.entity_label_config.Key, dev.projects.entity_label_config.Value,
            KeyValue<CommunityEntityLabelConfigKey, CommunityEntityLabelConfigValue>> {

        private final String stateStoreName;
        private final AvroSerdes avroSerdes;

        public TransformSupplier(String stateStoreName, AvroSerdes avroSerdes) {
            this.stateStoreName = stateStoreName;
            this.avroSerdes = avroSerdes;
        }

        @Override
        public Transformer<dev.projects.entity_label_config.Key, dev.projects.entity_label_config.Value, KeyValue<CommunityEntityLabelConfigKey, CommunityEntityLabelConfigValue>> get() {
            return new Transform(stateStoreName);
        }

        @Override
        public Set<StoreBuilder<?>> stores() {
            StoreBuilder<KeyValueStore<dev.projects.entity_label_config.Key, CommunityEntityLabelConfigValue>> keyValueStoreBuilder =
                    Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(stateStoreName),
                            avroSerdes.ProEntityLabelConfigKey(),
                            avroSerdes.CommunityEntityLabelConfigValue());
            return Collections.singleton(keyValueStoreBuilder);
        }
    }

    public static class Transform implements Transformer<
            dev.projects.entity_label_config.Key, dev.projects.entity_label_config.Value,
            KeyValue<CommunityEntityLabelConfigKey, CommunityEntityLabelConfigValue>> {

        private final String stateStoreName;
        private KeyValueStore<dev.projects.entity_label_config.Key, CommunityEntityLabelConfigValue> kvStore;

        private final ObjectMapper mapper = new ObjectMapper(); // create once, reuse

        public Transform(String stateStoreName) {
            this.stateStoreName = stateStoreName;
        }

        @Override
        public void init(ProcessorContext context) {
            this.kvStore = context.getStateStore(stateStoreName);
        }

        @Override
        public KeyValue<CommunityEntityLabelConfigKey, CommunityEntityLabelConfigValue> transform(
                dev.projects.entity_label_config.Key key,
                dev.projects.entity_label_config.Value value
        ) {

            // ignore non community items
            if (value != null && value.getFkProject() != null && value.getFkProject() != I.DEFAULT_PROJECT.get())
                return null;

            // if the item was deleted
            if (value == null || Utils.stringIsEqualTrue(value.getDeleted$1())) {
                // get existing value
                var existing = kvStore.get(key);
                if (existing == null) return null;

                // mark as deleted
                existing.setDeleted$1(true);

                // push downstream
                return KeyValue.pair(
                        CommunityEntityLabelConfigKey.newBuilder()
                                .setClassId(existing.getClassId())
                                .build(),
                        existing
                );
            }

            // if item was not deleted
            try {
                EntityLabelConfig config = mapper.readValue(value.getConfig(), EntityLabelConfig.class);
                var k = CommunityEntityLabelConfigKey.newBuilder()
                        .setClassId(value.getFkClass())
                        .build();
                var v = CommunityEntityLabelConfigValue.newBuilder()
                        .setClassId(value.getFkClass())
                        .setConfig(config)
                        .setDeleted$1(Utils.stringIsEqualTrue(value.getDeleted$1()))
                        .build();

                // add to local store
                kvStore.put(key, v);

                // push downstream
                return KeyValue.pair(k, v);

            } catch (JsonProcessingException e) {
                e.printStackTrace();
                return null;
            }


        }

        public void close() {

        }

    }

}
