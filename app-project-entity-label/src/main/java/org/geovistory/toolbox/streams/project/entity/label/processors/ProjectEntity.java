package org.geovistory.toolbox.streams.project.entity.label.processors;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.ProjectEntityKey;
import org.geovistory.toolbox.streams.avro.ProjectEntityValue;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.Utils;
import org.geovistory.toolbox.streams.project.entity.label.DbTopicNames;
import org.geovistory.toolbox.streams.project.entity.label.RegisterInputTopics;


public class ProjectEntity {

    public static void main(String[] args) {
        System.out.println(buildStandalone(new StreamsBuilder()).describe());
    }

    public static Topology buildStandalone(StreamsBuilder builder) {
        var registerInputTopic = new RegisterInputTopics(builder);

        return addProcessors(
                builder,
                registerInputTopic.infResourceTable(),
                registerInputTopic.proInfoProjRelTable()
        ).builder().build();
    }

    public static ProjectEntityReturnValue addProcessors(
            StreamsBuilder builder,
            KTable<dev.information.resource.Key, dev.information.resource.Value> infResourceTable,
            KTable<dev.projects.info_proj_rel.Key, dev.projects.info_proj_rel.Value> proInfoProjRelTable) {

        var avroSerdes = new ConfluentAvroSerdes();


        /* STREAM PROCESSORS */
        // 2)
        // https://stackoverflow.com/questions/62884230/ktable-ktable-foreign-key-join-not-producing-all-messages-when-topics-have-more
        var projectEntityJoin = proInfoProjRelTable.join(
                infResourceTable,
                value -> dev.information.resource.Key.newBuilder()
                        .setPkEntity(value.getFkEntity())
                        .build(),
                (value1, value2) -> {
                    if (value2.getFkClass() == null) return null;
                    var v1Deleted = Utils.stringIsEqualTrue(value1.getDeleted$1());
                    var v2Deleted = Utils.stringIsEqualTrue(value2.getDeleted$1());
                    var notInProject = !value1.getIsInProject();
                    var deleted = v1Deleted || v2Deleted || notInProject;
                    return ProjectEntityValue.newBuilder()
                            .setProjectId(value1.getFkProject())
                            .setEntityId("i" + value1.getFkEntity())
                            .setClassId(value2.getFkClass())
                            .setDeleted$1(deleted)
                            .build();
                },
                TableJoined.as(inner.TOPICS.project_entity_join+ "-fk-join"),
                Materialized.<dev.projects.info_proj_rel.Key, ProjectEntityValue, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.project_entity_join)
                        .withKeySerde(avroSerdes.ProInfoProjRelKey())
                        .withValueSerde(avroSerdes.ProjectEntityValue())
        );

        var projectEntityStream = projectEntityJoin
                .toStream(
                        Named.as(inner.TOPICS.project_entity_join + "-to-stream")
                )
                .map((key, value) -> {
                    var k = ProjectEntityKey.newBuilder()
                            .setProjectId(key.getFkProject())
                            .setEntityId("i" + key.getFkEntity())
                            .build();
                    var v = ProjectEntityValue.newBuilder()
                            .setProjectId(key.getFkProject())
                            .setEntityId("i" + key.getFkEntity())
                            .setClassId(value.getClassId())
                            .setDeleted$1(value.getDeleted$1())
                            .build();
                    return KeyValue.pair(k, v);
                },
                        Named.as("kstream-map-project-entity")
                );

        /* SINK PROCESSORS */

        projectEntityStream.to(output.TOPICS.project_entity,
                Produced.with(avroSerdes.ProjectEntityKey(), avroSerdes.ProjectEntityValue())
                        .withName(output.TOPICS.project_entity + "-producer")
        );

        return new ProjectEntityReturnValue(builder, projectEntityStream);

    }


    public enum input {
        TOPICS;
        public final String pro_info_proj_rel = DbTopicNames.pro_info_proj_rel.getName();
        public final String inf_resource = DbTopicNames.inf_resource.getName();
    }


    public enum inner {
        TOPICS;
        public final String project_entity_join = "project_entity_join";
    }

    public enum output {
        TOPICS;
        public final String project_entity = Utils.tsPrefixed("project_entity");
    }

}