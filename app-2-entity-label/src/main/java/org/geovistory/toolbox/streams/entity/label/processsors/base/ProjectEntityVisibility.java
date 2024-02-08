package org.geovistory.toolbox.streams.entity.label.processsors.base;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.ProjectEntityKey;
import org.geovistory.toolbox.streams.avro.ProjectEntityVisibilityValue;
import org.geovistory.toolbox.streams.entity.label.AvroSerdes;
import org.geovistory.toolbox.streams.entity.label.OutputTopicNames;
import org.geovistory.toolbox.streams.entity.label.RegisterInnerTopic;
import org.geovistory.toolbox.streams.entity.label.RegisterInputTopic;
import org.geovistory.toolbox.streams.lib.Utils;
import org.geovistory.toolbox.streams.lib.jsonmodels.CommunityVisibility;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;


@ApplicationScoped
public class ProjectEntityVisibility {


    @Inject
    AvroSerdes avroSerdes;

    @Inject
    RegisterInputTopic registerInputTopic;
    @Inject
    RegisterInnerTopic registerInnerTopic;

    @Inject
    OutputTopicNames outputTopicNames;

    public ProjectEntityVisibility(AvroSerdes avroSerdes, RegisterInputTopic registerInputTopic, RegisterInnerTopic registerInnerTopic, OutputTopicNames outputTopicNames) {
        this.avroSerdes = avroSerdes;
        this.registerInputTopic = registerInputTopic;
        this.registerInnerTopic = registerInnerTopic;
        this.outputTopicNames = outputTopicNames;
    }

    public void addProcessorsStandalone() {
        addProcessors(
                registerInputTopic.infResourceTable(),
                registerInputTopic.proInfoProjRelTable()
        );
    }

    public ProjectEntityVisibilityReturnValue addProcessors(
            KTable<ts.information.resource.Key, ts.information.resource.Value> infResourceTable,
            KTable<ts.projects.info_proj_rel.Key, ts.projects.info_proj_rel.Value> proInfoProjRelTable) {

        var mapper = new ObjectMapper();

        /* STREAM PROCESSORS */
        // 2)
        // https://stackoverflow.com/questions/62884230/ktable-ktable-foreign-key-join-not-producing-all-messages-when-topics-have-more
        var projectEntityJoin = proInfoProjRelTable.join(
                infResourceTable,
                value -> ts.information.resource.Key.newBuilder()
                        .setPkEntity(value.getFkEntity())
                        .build(),
                (value1, value2) -> {
                    if (value2.getFkClass() == null) return null;
                    if (value2.getCommunityVisibility() == null) return null;
                    var v1Deleted = Utils.stringIsEqualTrue(value1.getDeleted$1());
                    var v2Deleted = Utils.stringIsEqualTrue(value2.getDeleted$1());
                    var notInProject = value1.getIsInProject() == null || !value1.getIsInProject();
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
                Materialized.<ts.projects.info_proj_rel.Key, ProjectEntityVisibilityValue, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.project_entity_visibilty_join)
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

        projectEntityVisibilityStream.to(outputTopicNames.projectEntityVisibility(),
                Produced.with(avroSerdes.ProjectEntityKey(), avroSerdes.ProjectEntityVisibilityValue())
                        .withName(outputTopicNames.projectEntityVisibility() + "-producer")
        );

        return new ProjectEntityVisibilityReturnValue(projectEntityVisibilityStream);

    }

    public enum inner {
        TOPICS;
        public final String project_entity_visibilty_join = "project_entity_visibilty_join";
    }


}
