package org.geovistory.toolbox.streams.topologies;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.projects.entity_label_config.Key;
import dev.projects.entity_label_config.Value;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.app.DbTopicNames;
import org.geovistory.toolbox.streams.app.RegisterInputTopic;
import org.geovistory.toolbox.streams.app.RegisterOutputTopic;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.Utils;


public class ProjectEntityLabelConfig {

    public static void main(String[] args) {
        System.out.println(buildStandalone(new StreamsBuilder()).describe());
    }

    public static Topology buildStandalone(StreamsBuilder builder) {
        var registerInputTopic = new RegisterInputTopic(builder);
        var registerOutputTopic = new RegisterOutputTopic(builder);

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
        var mapper = new ObjectMapper(); // create once, reuse
        var avroSerdes = new ConfluentAvroSerdes();
        /* STREAM PROCESSORS */
        // 2)

        var configByProjectClassKey = proEntityLabelConfigStream.selectKey(
                        (key, value) -> ProjectClassKey.newBuilder()
                                .setProjectId(value.getFkProject())
                                .setClassId(value.getFkClass())
                                .build(),
                        Named.as(inner.TOPICS.project_entity_label_config_by_project_class)
                )
                .toTable(
                        Materialized.<ProjectClassKey, Value, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.project_entity_label_config_by_project_class)
                                .withKeySerde(avroSerdes.ProjectClassKey())
                                .withValueSerde(avroSerdes.ProEntityLabelConfigValue())
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
                        result.setDeleted$1(Utils.stringIsEqualTrue(value2.getDeleted$1()));
                        try {
                            var config = mapper.readValue(value2.getConfig(), EntityLabelConfig.class);
                            result.setConfig(config);
                        } catch (JsonProcessingException e) {
                            e.printStackTrace();
                        }
                    }
                    return result;
                },
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
                Materialized.<ProjectClassKey, ProjectEntityLabelConfigValue, KeyValueStore<Bytes, byte[]>>as(output.TOPICS.project_entity_label_config_enriched)
                        .withKeySerde(avroSerdes.ProjectClassKey())
                        .withValueSerde(avroSerdes.ProjectEntityLabelConfigValue())
        );

        projectEntityLabelConfigEnrichedTable.toStream().to(
                output.TOPICS.project_entity_label_config_enriched,
                Produced.with(
                        avroSerdes.ProjectClassKey(),
                        avroSerdes.ProjectEntityLabelConfigValue()
                )
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
        public final String project_entity_label_config_enriched = Utils.tsPrefixed("project_entity_label_config_enriched");
    }

}
