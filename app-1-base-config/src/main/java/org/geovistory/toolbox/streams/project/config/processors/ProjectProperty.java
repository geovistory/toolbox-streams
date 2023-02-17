package org.geovistory.toolbox.streams.project.config.processors;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.Utils;
import org.geovistory.toolbox.streams.project.config.Env;
import org.geovistory.toolbox.streams.project.config.RegisterInnerTopic;
import org.geovistory.toolbox.streams.project.config.RegisterInputTopic;

import java.util.LinkedList;
import java.util.Objects;


public class ProjectProperty {

    public static void main(String[] args) {
        System.out.println(buildStandalone(new StreamsBuilder()).describe());
    }

    public static Topology buildStandalone(StreamsBuilder builder) {
        var registerInputTopic = new RegisterInputTopic(builder);
        var registerInnerTopic = new RegisterInnerTopic(builder);

        return addProcessors(
                builder,
                registerInputTopic.ontomePropertyStream(),
                registerInnerTopic.projectProfileStream()
        ).builder().build();
    }

    public static ProjectPropertyReturnValue addProcessors(
            StreamsBuilder builder,
            KStream<OntomePropertyKey, OntomePropertyValue> ontomePropertyStream,
            KStream<ProjectProfileKey, ProjectProfileValue> projectProfileStream) {

        var avroSerdes = new ConfluentAvroSerdes();


        /* STREAM PROCESSORS */

        // 3) GroupBy
        var propertyByProfileIdGrouped = ontomePropertyStream
                .groupBy(
                        (key, value) -> value.getDfhFkProfile(),
                        Grouped.with(
                                Serdes.Integer(), avroSerdes.OntomePropertyValue()
                        ));
        // 3) Aggregate
        var propertyByProfileIdAggregated = propertyByProfileIdGrouped.aggregate(
                () -> ProfilePropertyMap.newBuilder().build(),
                (aggKey, newValue, aggValue) -> {
                    var key = newValue.getDfhFkProfile() + "_" + newValue.getDfhPkProperty() + "_" + newValue.getDfhPropertyDomain() + "_" + newValue.getDfhPropertyRange();
                    var value = ProfileProperty.newBuilder()
                            .setProfileId(newValue.getDfhFkProfile())
                            .setPropertyId(newValue.getDfhPkProperty())
                            .setDomainId(newValue.getDfhPropertyDomain())
                            .setRangeId(newValue.getDfhPropertyRange())
                            .setDeleted$1(Objects.equals(newValue.getDeleted$1(), "true"))
                            .build();
                    aggValue.getMap().put(key, value);
                    return aggValue;
                },
                Named.as(inner.TOPICS.profile_with_properties)
                ,
                Materialized.<Integer, ProfilePropertyMap, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.profile_with_properties)
                        .withKeySerde(Serdes.Integer())
                        .withValueSerde(avroSerdes.ProfilePropertyMapValue())
        );


        // 4)
        var projectProfileTable = projectProfileStream
                .toTable(
                        Named.as(inner.TOPICS.profile_with_properties + "-to-table"),
                        Materialized
                                .<ProjectProfileKey, ProjectProfileValue, KeyValueStore<Bytes, byte[]>>
                                        as(inner.TOPICS.profile_with_properties + "-store")
                                .withKeySerde(avroSerdes.ProjectProfileKey())
                                .withValueSerde(avroSerdes.ProjectProfileValue())
                );

        // 5)
        var projectPropertiesPerProfile = projectProfileTable.join(
                propertyByProfileIdAggregated,
                ProjectProfileValue::getProfileId,
                (projectProfileValue, profilePropertyMap) -> {
                    var projectPropertyMap = ProjectPropertyMap.newBuilder().build();
                    profilePropertyMap.getMap().values()
                            .forEach(property -> {
                                var projectId = projectProfileValue.getProjectId();
                                var projectPropertyIsDeleted = projectProfileValue.getDeleted$1() || property.getDeleted$1();

                                var v = ProjectPropertyValue.newBuilder()
                                        .setProjectId(projectId)
                                        .setDomainId(property.getDomainId())
                                        .setPropertyId(property.getPropertyId())
                                        .setRangeId(property.getRangeId())
                                        .setDeleted$1(projectPropertyIsDeleted)
                                        .build();
                                var key = projectId + "_" + property.getDomainId() + "_" + property.getPropertyId() + "_" + property.getRangeId();
                                // ... and add one project-property
                                projectPropertyMap.getMap().put(key, v);
                            });
                    return projectPropertyMap;
                },
                TableJoined.as("project_properties_per_profile"+ "-fk-join")
        );

// 3)

        var projectPropertyStream = projectPropertiesPerProfile
                .toStream(
                        Named.as(inner.TOPICS.project_properties_stream)
                )
                .flatMap((key, value) -> {
                            var list = new LinkedList<KeyValue<ProjectPropertyKey, ProjectPropertyValue>>();
                            if (value != null && value.getMap() != null) {
                                for (var projectPropertyValue : value.getMap().values()) {
                                    var k = ProjectPropertyKey.newBuilder()
                                            .setPropertyId(projectPropertyValue.getPropertyId())
                                            .setProjectId(projectPropertyValue.getProjectId())
                                            .setDomainId(projectPropertyValue.getDomainId())
                                            .setRangeId(projectPropertyValue.getRangeId())
                                            .build();
                                    var item = KeyValue.pair(k, projectPropertyValue);
                                    list.add(item);
                                }
                            }
                            return list;
                        },
                        Named.as(inner.TOPICS.project_properties_flat));

        projectPropertyStream.to(output.TOPICS.project_property,
                Produced.with(avroSerdes.ProjectPropertyKey(), avroSerdes.ProjectPropertyValue())
                        .withName(output.TOPICS.project_property + "-producer")
        );

        return new ProjectPropertyReturnValue(builder, projectPropertyStream);

    }


    public enum input {
        TOPICS;
        public final String project_profile = ProjectProfiles.output.TOPICS.project_profile;
        public final String ontome_property = Env.INSTANCE.TOPIC_ONTOME_PROPERTY;
    }


    public enum inner {
        TOPICS;
        public final String profile_with_properties = "profile_with_properties";
        public final String project_properties_stream = "project_properties_stream";
        public final String project_properties_flat = "project_properties_flat";
    }

    public enum output {
        TOPICS;
        public final String project_property = Utils.tsPrefixed("project_property");
    }

}
