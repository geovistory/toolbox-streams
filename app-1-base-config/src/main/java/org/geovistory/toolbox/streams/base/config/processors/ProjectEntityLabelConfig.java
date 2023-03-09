package org.geovistory.toolbox.streams.base.config.processors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.projects.entity_label_config.Key;
import dev.projects.entity_label_config.Value;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.base.config.DbTopicNames;
import org.geovistory.toolbox.streams.base.config.RegisterInnerTopic;
import org.geovistory.toolbox.streams.base.config.RegisterInputTopic;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.Utils;

import java.util.Collections;
import java.util.Set;


public class ProjectEntityLabelConfig {

    public static void main(String[] args) {
        System.out.println(buildStandalone(new StreamsBuilder()).describe());
    }

    public static Topology buildStandalone(StreamsBuilder builder) {
        var registerInputTopic = new RegisterInputTopic(builder);
        var registerOutputTopic = new RegisterInnerTopic(builder);

        return addProcessors(
                builder,
                registerOutputTopic.projectClassTable(),
                registerInputTopic.proEntityLabelConfigStream(),
                registerOutputTopic.communityEntityLabelConfigTable()
        ).builder().build();
    }


    public static ProjectEntityLabelConfigReturnValue addProcessors(
            StreamsBuilder builder,
            KTable<ProjectClassKey, ProjectClassValue> projectClassTable,
            KStream<Key, Value> proEntityLabelConfigStream,
            KTable<CommunityEntityLabelConfigKey, CommunityEntityLabelConfigValue> communityEntityLabelConfigTable
    ) {
        var avroSerdes = new ConfluentAvroSerdes();
        /* STREAM PROCESSORS */
        // 2)

        var configByProjectClassKey = proEntityLabelConfigStream
                .transform(new TransformSupplier("handle_project_entity_label_config_deletes"))
                .repartition(
                        Repartitioned.<ProjectClassKey, ProjectEntityLabelConfigValue>as(inner.TOPICS.project_entity_label_config_by_project_class + "-repartition")
                                .withKeySerde(avroSerdes.ProjectClassKey())
                                .withValueSerde(avroSerdes.ProjectEntityLabelConfigValue())
                                .withName(inner.TOPICS.project_entity_label_config_by_project_class + "-repartition")
                )
                .toTable(
                        Named.as(inner.TOPICS.project_entity_label_config_by_project_class + "-to-table"),
                        Materialized.<ProjectClassKey, ProjectEntityLabelConfigValue, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.project_entity_label_config_by_project_class)
                                .withKeySerde(avroSerdes.ProjectClassKey())
                                .withValueSerde(avroSerdes.ProjectEntityLabelConfigValue())
                );

        // 2
        // Join project config
        var projectClassWithConfig = projectClassTable.leftJoin(configByProjectClassKey,
                (value1, value2) -> {
                    var result = ProjectEntityLabelConfigValue.newBuilder()
                            .setProjectId(value1.getProjectId())
                            .setClassId(value1.getClassId())
                            .build();
                    // if we have a project configuration, take it
                    if (value2 != null) {
                        result.setDeleted$1(value2.getDeleted$1());
                        result.setConfig(value2.getConfig());
                    }
                    return result;
                },
                Named.as(inner.TOPICS.project_class_with_project_label_config + "-fk-left-join"),
                Materialized.<ProjectClassKey, ProjectEntityLabelConfigValue, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.project_class_with_project_label_config)
                        .withKeySerde(avroSerdes.ProjectClassKey())
                        .withValueSerde(avroSerdes.ProjectEntityLabelConfigValue())
        );

        // 3
        // Join community config
        var projectEntityLabelConfigEnrichedTable = projectClassWithConfig.leftJoin(communityEntityLabelConfigTable,
                value -> CommunityEntityLabelConfigKey.newBuilder()
                        .setClassId(value.getClassId())
                        .build(),
                (projectConfig, communityConfig) -> {
                    var result = ProjectEntityLabelConfigValue.newBuilder()
                            .setClassId(projectConfig.getClassId())
                            .setProjectId(projectConfig.getProjectId())
                            .setDeleted$1(true);

                    // if we have a project configuration, take it
                    if (projectConfig.getConfig() != null && Utils.booleanIsNotEqualTrue(projectConfig.getDeleted$1())) {
                        result.setConfig(projectConfig.getConfig());
                        result.setDeleted$1(false);
                    }

                    // else if we have the community config, take it
                    else if (communityConfig != null && Utils.booleanIsNotEqualTrue(communityConfig.getDeleted$1())) {
                        result.setConfig(communityConfig.getConfig());
                        result.setDeleted$1(false);
                    }
                    return result.build();
                },
                TableJoined.as(output.TOPICS.project_entity_label_config + "-fk-left-join"),
                Materialized.<ProjectClassKey, ProjectEntityLabelConfigValue, KeyValueStore<Bytes, byte[]>>as(output.TOPICS.project_entity_label_config)
                        .withKeySerde(avroSerdes.ProjectClassKey())
                        .withValueSerde(avroSerdes.ProjectEntityLabelConfigValue())
        );

        projectEntityLabelConfigEnrichedTable.toStream(
                Named.as(output.TOPICS.project_entity_label_config + "-to-stream")
        ).to(
                output.TOPICS.project_entity_label_config,
                Produced.with(
                                avroSerdes.ProjectClassKey(),
                                avroSerdes.ProjectEntityLabelConfigValue()
                        )
                        .withName(output.TOPICS.project_entity_label_config + "-producer")
        );


        return new ProjectEntityLabelConfigReturnValue(builder, projectEntityLabelConfigEnrichedTable);

    }

    public enum input {
        TOPICS;
        public final String pro_entity_label_config = DbTopicNames.pro_entity_label_config.getName();
        public final String project_class = ProjectClass.output.TOPICS.project_class;
        public final String community_entity_label_config = CommunityEntityLabelConfig.output.TOPICS.community_entity_label_config;
    }

    public enum inner {
        TOPICS;
        public final String project_entity_label_config_by_project_class = Utils.tsPrefixed("project_entity_label_config_by_project_class");
        public final String project_class_with_project_label_config = Utils.tsPrefixed("project_class_with_project_label_config");
    }

    public enum output {
        TOPICS;
        public final String project_entity_label_config = Utils.tsPrefixed("project_entity_label_config");
    }


    public static class TransformSupplier implements TransformerSupplier<
            dev.projects.entity_label_config.Key, dev.projects.entity_label_config.Value,
            KeyValue<ProjectClassKey, ProjectEntityLabelConfigValue>> {

        private final String stateStoreName;
        private final ConfluentAvroSerdes avroSerdes = new ConfluentAvroSerdes();

        TransformSupplier(String stateStoreName) {
            this.stateStoreName = stateStoreName;
        }

        @Override
        public Transformer<dev.projects.entity_label_config.Key, dev.projects.entity_label_config.Value, KeyValue<ProjectClassKey, ProjectEntityLabelConfigValue>> get() {
            return new ProjectEntityLabelConfig.Transform(stateStoreName);
        }

        @Override
        public Set<StoreBuilder<?>> stores() {
            StoreBuilder<KeyValueStore<dev.projects.entity_label_config.Key, ProjectEntityLabelConfigValue>> keyValueStoreBuilder =
                    Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(stateStoreName),
                            avroSerdes.ProEntityLabelConfigKey(),
                            avroSerdes.ProjectEntityLabelConfigValue());
            return Collections.singleton(keyValueStoreBuilder);
        }
    }

    public static class Transform implements Transformer<
            dev.projects.entity_label_config.Key, dev.projects.entity_label_config.Value,
            KeyValue<ProjectClassKey, ProjectEntityLabelConfigValue>> {

        private final String stateStoreName;
        private KeyValueStore<dev.projects.entity_label_config.Key, ProjectEntityLabelConfigValue> kvStore;

        private final ObjectMapper mapper = new ObjectMapper(); // create once, reuse

        public Transform(String stateStoreName) {
            this.stateStoreName = stateStoreName;
        }

        @Override
        public void init(ProcessorContext context) {
            this.kvStore = context.getStateStore(stateStoreName);
        }

        @Override
        public KeyValue<ProjectClassKey, ProjectEntityLabelConfigValue> transform(
                dev.projects.entity_label_config.Key key,
                dev.projects.entity_label_config.Value value
        ) {

            // if the item was deleted
            if (value == null || Utils.stringIsEqualTrue(value.getDeleted$1())) {
                // get existing value
                var existing = kvStore.get(key);
                if (existing == null) return null;

                // mark as deleted
                existing.setDeleted$1(true);

                // push downstream
                return KeyValue.pair(
                        ProjectClassKey.newBuilder()
                                .setClassId(existing.getClassId())
                                .setProjectId(existing.getProjectId())
                                .build(),
                        existing
                );
            }

            // if item was not deleted
            try {
                EntityLabelConfig config = mapper.readValue(value.getConfig(), EntityLabelConfig.class);
                var k = ProjectClassKey.newBuilder()
                        .setClassId(value.getFkClass())
                        .setProjectId(value.getFkProject())
                        .build();
                var v = ProjectEntityLabelConfigValue.newBuilder()
                        .setClassId(value.getFkClass())
                        .setProjectId(value.getFkProject())
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
