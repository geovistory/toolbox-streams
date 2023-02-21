package org.geovistory.toolbox.streams.entity.label.processsors.base;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.ProjectEntityKey;
import org.geovistory.toolbox.streams.avro.ProjectEntityVisibilityValue;
import org.geovistory.toolbox.streams.entity.label.DbTopicNames;
import org.geovistory.toolbox.streams.entity.label.RegisterInputTopics;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.Utils;
import org.geovistory.toolbox.streams.lib.jsonmodels.CommunityVisibility;


public class ProjectEntityVisibility {

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

    public static ProjectEntityVisibilityReturnValue addProcessors(
            StreamsBuilder builder,
            KTable<dev.information.resource.Key, dev.information.resource.Value> infResourceTable,
            KTable<dev.projects.info_proj_rel.Key, dev.projects.info_proj_rel.Value> proInfoProjRelTable) {

        var avroSerdes = new ConfluentAvroSerdes();

        var mapper = new ObjectMapper();

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
                    var communityCanSeeInToolbox = false;
                    var communityCanSeeInDataApi = false;
                    var communityCanSeeInWebsite = false;
                    try {
                        var communitVisibility = mapper.readValue(value2.getCommunityVisibility(), CommunityVisibility.class);
                        if (communitVisibility.toolbox) communityCanSeeInToolbox = true;
                        if (communitVisibility.dataApi) communityCanSeeInDataApi = true;
                        if (communitVisibility.website) communityCanSeeInWebsite = true;
                    } catch (JsonProcessingException e) {
                        System.out.println("ERROR parsing the community visibility: ");
                        e.printStackTrace();
                    }

                    return ProjectEntityVisibilityValue.newBuilder()
                            .setProjectId(value1.getFkProject())
                            .setEntityId("i" + value1.getFkEntity())
                            .setClassId(value2.getFkClass())
                            .setCommunityVisibilityToolbox(communityCanSeeInToolbox)
                            .setCommunityVisibilityDataApi(communityCanSeeInDataApi)
                            .setCommunityVisibilityWebsite(communityCanSeeInWebsite)
                            .setDeleted$1(deleted)
                            .build();
                },
                TableJoined.as(inner.TOPICS.project_entity_visibilty_join + "-fk-join"),
                Materialized.<dev.projects.info_proj_rel.Key, ProjectEntityVisibilityValue, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.project_entity_visibilty_join)
                        .withKeySerde(avroSerdes.ProInfoProjRelKey())
                        .withValueSerde(avroSerdes.ProjectEntityVisibilityValue())
        );

        var projectEntityVisibilityStream = projectEntityJoin
                .toStream(
                        Named.as(inner.TOPICS.project_entity_visibilty_join + "-to-stream")
                )
                .selectKey((key, value) -> ProjectEntityKey.newBuilder()
                                .setProjectId(key.getFkProject())
                                .setEntityId("i" + key.getFkEntity())
                                .build(),
                        Named.as("kstream-select-key-project-entity")
                );

        /* SINK PROCESSORS */

        projectEntityVisibilityStream.to(output.TOPICS.project_entity_visibility,
                Produced.with(avroSerdes.ProjectEntityKey(), avroSerdes.ProjectEntityVisibilityValue())
                        .withName(output.TOPICS.project_entity_visibility + "-producer")
        );

        return new ProjectEntityVisibilityReturnValue(builder, projectEntityVisibilityStream);

    }


    public enum input {
        TOPICS;
        public final String pro_info_proj_rel = DbTopicNames.pro_info_proj_rel.getName();
        public final String inf_resource = DbTopicNames.inf_resource.getName();
    }


    public enum inner {
        TOPICS;
        public final String project_entity_visibilty_join = "project_entity_visibilty_join";
    }

    public enum output {
        TOPICS;
        public final String project_entity_visibility = Utils.tsPrefixed("project_entity_visibility");
    }

}
