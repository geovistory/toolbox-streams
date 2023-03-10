package org.geovistory.toolbox.streams.base.config.processors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
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
import org.geovistory.toolbox.streams.base.config.DbTopicNames;
import org.geovistory.toolbox.streams.base.config.I;
import org.geovistory.toolbox.streams.base.config.RegisterInputTopic;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.Utils;

import java.util.Collections;
import java.util.Set;


public class CommunityEntityLabelConfig {

    public static void main(String[] args) {
        System.out.println(buildStandalone(new StreamsBuilder()).describe());
    }

    public static Topology buildStandalone(StreamsBuilder builder) {
        var registerInputTopic = new RegisterInputTopic(builder);

        return addProcessors(
                builder,
                registerInputTopic.proEntityLabelConfigStream()
        ).builder().build();
    }


    public static CommunityEntityLabelConfigReturnValue addProcessors(
            StreamsBuilder builder,
            KStream<dev.projects.entity_label_config.Key, dev.projects.entity_label_config.Value> proEntityLabelConfigStream
    ) {
        var avroSerdes = new ConfluentAvroSerdes();
        /* STREAM PROCESSORS */
        // 2)
        var communityEntityLabelConfigStream = proEntityLabelConfigStream.transform(
                new TransformSupplier("kstream-flatmap-project-entity-label-config-to-community-entity-label-config")
        );
        /* SINK PROCESSORS */

        // 8) to
        communityEntityLabelConfigStream.to(output.TOPICS.community_entity_label_config,
                Produced.with(avroSerdes.CommunityEntityLabelConfigKey(), avroSerdes.CommunityEntityLabelConfigValue())
                        .withName(output.TOPICS.community_entity_label_config + "-producer")
        );

        return new CommunityEntityLabelConfigReturnValue(builder, communityEntityLabelConfigStream);

    }

    public enum input {
        TOPICS;
        public final String entity_label_config = DbTopicNames.pro_entity_label_config.getName();
    }

    public enum output {
        TOPICS;
        public final String community_entity_label_config = Utils.tsPrefixed("community_entity_label_config");
    }

    public static class TransformSupplier implements TransformerSupplier<
            dev.projects.entity_label_config.Key, dev.projects.entity_label_config.Value,
            KeyValue<CommunityEntityLabelConfigKey, CommunityEntityLabelConfigValue>> {

        private final String stateStoreName;
        private final ConfluentAvroSerdes avroSerdes = new ConfluentAvroSerdes();

        TransformSupplier(String stateStoreName) {
            this.stateStoreName = stateStoreName;
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
