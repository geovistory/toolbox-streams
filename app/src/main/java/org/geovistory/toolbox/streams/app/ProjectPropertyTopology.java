package org.geovistory.toolbox.streams.app;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
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
        KTable<dev.data_for_history.api_property.Key, dev.data_for_history.api_property.Value> apiProperty = builder
                .table(input.TOPICS.api_property,
                        Consumed.with(avroSerdes.DfhApiPropertyKey(), avroSerdes.DfhApiPropertyValue()));

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


        KGroupedTable<Integer, dev.data_for_history.api_property.Value> groupedTable = apiProperty.groupBy(
                (key, value) -> KeyValue.pair(value.getDfhFkProfile(), value),
                Grouped.with(
                        Serdes.Integer(), avroSerdes.DfhApiPropertyValue()
                ));
        var profileWithProperties = groupedTable.aggregate(
                ArrayList::new,
                (aggKey, newValue, aggValue) -> {
                    aggValue.add(newValue);
                    return aggValue;
                },
                (aggKey, oldValue, aggValue) -> aggValue,
                Named.as(inner.TOPICS.profile_with_properties),
                Materialized.with(Serdes.Integer(), listSerdes.DfhApiPropertyValueList())
        );


        var projectPropertiesPerProfile = profileWithProperties.join(
                profileWithProjects,
                (propertiesOfProfile, projectsOfProfile) -> {
                    List<ProjectPropertyValue> projectPropertyValues = new ArrayList<>();
                    propertiesOfProfile.forEach(property -> projectsOfProfile.getItem()
                            .forEach((projectId, projectIsDeleted) -> {
                                var propertyIsDeleted = Objects.equals(property.getDeleted$1(), "true");
                                var projectPropertyIsDeleted = projectIsDeleted || propertyIsDeleted;
                                var v = ProjectPropertyValue.newBuilder()
                                        .setProjectId(Integer.parseInt(projectId))
                                        .setDomainId(property.getDfhPropertyDomain())
                                        .setPropertyId(property.getDfhPkProperty())
                                        .setRangeId(property.getDfhPropertyRange())
                                        .setDeleted$1(projectPropertyIsDeleted)
                                        .build();
                                projectPropertyValues.add(v);
                            }));
                    return projectPropertyValues;
                },
                Materialized.with(Serdes.Integer(), listSerdes.ProjectPropertyValueList())
        );

// 3)

        var projectPropertyStream = projectPropertiesPerProfile
                .toStream()
                .flatMap((key, value) -> value.stream().map(projectPropertyValue -> {
                    var k = ProjectPropertyKey.newBuilder()
                            .setPropertyId(projectPropertyValue.getPropertyId())
                            .setProjectId(projectPropertyValue.getProjectId())
                            .setDomainId(projectPropertyValue.getDomainId())
                            .setRangeId(projectPropertyValue.getRangeId())
                            .build();
                    return KeyValue.pair(k, projectPropertyValue);
                }).toList());

        projectPropertyStream.to(output.TOPICS.project_property,
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
        public final String profile_with_projects = Utils.tsPrefixed("profile_with_projects");
        public final String profile_with_properties = Utils.tsPrefixed("profile_with_properties");
    }

    public enum output {
        TOPICS;
        public final String project_property = Utils.tsPrefixed("project_property");
    }

}
