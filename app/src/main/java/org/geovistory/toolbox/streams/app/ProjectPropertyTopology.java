package org.geovistory.toolbox.streams.app;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.lib.AvroSerdes;
import org.geovistory.toolbox.streams.lib.ListSerdes;
import org.geovistory.toolbox.streams.lib.Utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;


public class ProjectPropertyTopology {

    public static void main(String[] args) {
        System.out.println(buildStandalone(new StreamsBuilder()).describe());
    }

    public static Topology buildStandalone(StreamsBuilder builder) {
        var avroSerdes = new AvroSerdes();
        // 1)
        // register project_profile
        var projectProfile = builder
                .stream(input.TOPICS.project_profile,
                        Consumed.with(avroSerdes.ProjectProfileKey(), avroSerdes.ProjectProfileValue()));

        return addProcessors(builder, projectProfile).build();
    }

    public static StreamsBuilder addProcessors(StreamsBuilder builder, KStream<ProjectProfileKey, ProjectProfileValue> projectProfile) {

        var avroSerdes = new AvroSerdes();
        var listSerdes = new ListSerdes();

        /* SOURCE PROCESSORS */

        // register api_property
        KTable<dev.data_for_history.api_property.Key, ProfileProperty> apiProperty = builder
                .table(input.TOPICS.api_property,
                        Consumed.with(avroSerdes.DfhApiPropertyKey(), avroSerdes.DfhApiPropertyValue()))
                .mapValues((readOnlyKey, value) -> ProfileProperty.newBuilder()
                        .setProfileId(value.getDfhFkProfile())
                        .setPropertyId(value.getDfhPkProperty())
                        .setDomainId(value.getDfhPropertyDomain())
                        .setRangeId(value.getDfhPropertyRange())
                        .setDeleted$1(Objects.equals(value.getDeleted$1(), "true"))
                        .build()
                );

        /* STREAM PROCESSORS */

// 2)

        var projectsByProfile = projectProfile
                .toTable(
                        Materialized.with(avroSerdes.ProjectProfileKey(), avroSerdes.ProjectProfileValue())
                )
                .groupBy((key, value) -> {
                            /*var v = BooleanMap.newBuilder().build();
                            v.getItem().put(value.getProjectId() + "", value.getDeleted$1());*/
                            return KeyValue.pair(key.getProfileId(), value);
                        },
                        Grouped.with(Serdes.Integer(), avroSerdes.ProjectProfileValue()));

        var profileWithProjects = projectsByProfile.aggregate(
                /* initializer */
                () -> BooleanMap.newBuilder().build(),
                /* adder */
                (aggKey, newValue, aggValue) -> {
                    aggValue.getItem().put(newValue.getProjectId() + "", newValue.getDeleted$1());
                    return aggValue;
                },
                /* subtractor */
                (aggKey, oldValue, aggValue) -> aggValue,
                Named.as(inner.TOPICS.profile_with_projects),
                Materialized.with(Serdes.Integer(), avroSerdes.BooleanMapValue())
        );

        KGroupedTable<Integer, ProfileProperty> groupedTable = apiProperty
                .groupBy(
                        (key, value) -> KeyValue.pair(value.getProfileId(), value),
                        Grouped.with(
                                Serdes.Integer(), avroSerdes.ProfilePropertyValue()
                        ));
        var profileWithProperties = groupedTable.aggregate(
                () -> ProfilePropertyMap.newBuilder().build(),
                (aggKey, newValue, aggValue) -> {
                    var key = newValue.getProfileId() + "_" + newValue.getPropertyId() + "_" + newValue.getDomainId() + "_" + newValue.getRangeId();
                    aggValue.getMap().put(key, newValue);
                    return aggValue;
                },
                (aggKey, oldValue, aggValue) -> aggValue,
                Named.as(inner.TOPICS.profile_with_properties)
                ,
                Materialized.<Integer, ProfilePropertyMap, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.profile_with_properties)
                        .withKeySerde(Serdes.Integer())
                        .withValueSerde(avroSerdes.ProfilePropertyMapValue())
        );


        // join properties (of a profile) with projects (of a profile) on the profile id
        KTable<Integer, List<ProjectPropertyValue>> projectPropertiesPerProfile = profileWithProperties.join(
                profileWithProjects,
                (propertiesOfProfile, projectsOfProfile) -> {

                    // declare list
                    List<ProjectPropertyValue> projectPropertyValues = new ArrayList<>();

                    // for each property of profile...
                    propertiesOfProfile.getMap().values().forEach(property -> projectsOfProfile
                            .getItem()

                            // ... loop over each project of the profile ...
                            .forEach((projectId, projectIsDeleted) -> {
                                var projectPropertyIsDeleted = projectIsDeleted || property.getDeleted$1();

                                var v = ProjectPropertyValue.newBuilder()
                                        .setProjectId(Integer.parseInt(projectId))
                                        .setDomainId(property.getDomainId())
                                        .setPropertyId(property.getPropertyId())
                                        .setRangeId(property.getRangeId())
                                        .setDeleted$1(projectPropertyIsDeleted)
                                        .build();

                                // ... and add one project-property
                                projectPropertyValues.add(v);
                            }));
                    return projectPropertyValues;
                },
                Named.as(inner.TOPICS.profile_with_project_properties),
                Materialized.<Integer, List<ProjectPropertyValue>, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.profile_with_project_properties)
                        .withKeySerde(Serdes.Integer())
                        .withValueSerde(listSerdes.ProjectPropertyValueList())
        );

// 3)

        var projectPropertyFlat = projectPropertiesPerProfile
                .toStream(
                        Named.as(inner.TOPICS.project_properties_stream)
                )
                .flatMap((key, value) -> value.stream().map(projectPropertyValue -> {
                                    var k = ProjectPropertyKey.newBuilder()
                                            .setPropertyId(projectPropertyValue.getPropertyId())
                                            .setProjectId(projectPropertyValue.getProjectId())
                                            .setDomainId(projectPropertyValue.getDomainId())
                                            .setRangeId(projectPropertyValue.getRangeId())
                                            .build();
                                    return KeyValue.pair(k, projectPropertyValue);
                                }
                        ).toList(),
                        Named.as(inner.TOPICS.project_properties_flat));

        projectPropertyFlat.to(output.TOPICS.project_property,
                Produced.with(avroSerdes.ProjectPropertyKey(), avroSerdes.ProjectPropertyValue()));

        return builder;

    }


    public enum input {
        TOPICS;
        public final String project_profile = ProjectProfilesTopology.output.TOPICS.project_profile;
        public final String api_property = Utils.dbPrefixed("data_for_history.api_property");
    }


    public enum inner {
        TOPICS;
        public final String profile_with_projects = "profile_with_projects";
        public final String profile_with_properties = "profile_with_properties";
        public final String profile_with_project_properties = "profile_with_project_properties";
        public final String project_properties_stream = "project_properties_stream";
        public final String project_properties_flat = "project_properties_flat";
    }

    public enum output {
        TOPICS;
        public final String project_property = Utils.tsPrefixed("project_property");
    }

}
