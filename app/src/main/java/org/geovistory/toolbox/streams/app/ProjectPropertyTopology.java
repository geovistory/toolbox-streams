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
                .mapValues((readOnlyKey, value) -> {
                    var v = BooleanMap.newBuilder().build();
                    v.getItem().put(value.getProjectId() + "", value.getDeleted$1());
                    return v;
                }).groupBy((key, value) -> key.getProfileId(),
                        Grouped.with(Serdes.Integer(), avroSerdes.BooleanMapValue()));
        var profileWithProjects = projectsByProfile.reduce((aggValue, newValue) -> {
            aggValue.getItem().forEach((projectId, __deleted) -> {
                // compare oldValue with newValue and mark projects of oldValue
                // as deleted, if they are absent in newValue
                if (!__deleted) newValue.getItem().putIfAbsent(projectId, true);
            });
            return newValue;
        });

        KGroupedTable<Integer, List<dev.data_for_history.api_property.Value>> groupedTable = apiProperty.groupBy(
                (key, value) -> {
                    List<dev.data_for_history.api_property.Value> returnVal = new ArrayList<>();
                    returnVal.add(value);
                    return KeyValue.pair(value.getDfhFkProfile(), returnVal);
                },
                Grouped.with(
                        Serdes.Integer(), listSerdes.DfhApiPropertyValueList()
                ));
        var profileWithProperties = groupedTable.reduce(
                (aggValue, newValue) -> {
                    aggValue.forEach((prop) -> {
                        // compare oldValue with newValue and mark items of oldValue
                        // as deleted if...
                        if (
                            // ...they are not marked as deleted in oldValue
                                !Objects.equals(prop.getDeleted$1(), "true")
                                        // ...and they are absent in newValue
                                        && newValue.stream().noneMatch(o -> o.getPkEntity() == prop.getPkEntity())
                        ) {
                            prop.setDeleted$1("true");
                            newValue.add(prop);
                        }
                    });
                    return newValue;
                },
                (aggValue, oldValue) -> aggValue
        );


        var projectPropertiesPerProfile = profileWithProperties.join(
                profileWithProjects,
                (propertiesOfProfile, projectsOfProfile) -> {
                    List<ProjectPropertyValue> projectPropertyValues = new ArrayList<>();
                    propertiesOfProfile.forEach(property -> projectsOfProfile.getItem().forEach((projectId, deleted) -> {
                        var markAsDeleted = deleted || Objects.equals(property.getDeleted$1(), "true");
                        var v = ProjectPropertyValue.newBuilder()
                                .setProjectId(Integer.parseInt(projectId))
                                .setDomainId(property.getDfhPropertyDomain())
                                .setPropertyId(property.getDfhPkProperty())
                                .setRangeId(property.getDfhPropertyRange())
                                .setDeleted$1(markAsDeleted)
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

    public enum output {
        TOPICS;
        public final String project_property = Utils.tsPrefixed("project_property");
    }

}
