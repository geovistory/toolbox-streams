package org.geovistory.toolbox.streams.app;

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

import java.util.Objects;


public class ProjectPropertyTopology {

    public static void main(String[] args) {
        System.out.println(buildStandalone(new StreamsBuilder()).describe());
    }

    public static Topology buildStandalone(StreamsBuilder builder) {
        var avroSerdes = new ConfluentAvroSerdes();
        // 1)
        // register project_profile
        var projectProfile = builder
                .stream(input.TOPICS.project_profile,
                        Consumed.with(avroSerdes.ProjectProfileKey(), avroSerdes.ProjectProfileValue()));

        return addProcessors(builder, projectProfile).build();
    }

    public static StreamsBuilder addProcessors(StreamsBuilder builder, KStream<ProjectProfileKey, ProjectProfileValue> projectProfile) {

        var avroSerdes = new ConfluentAvroSerdes();

        /* SOURCE PROCESSORS */

        // register api_property
        var apiPropertyTable = builder
                .table(input.TOPICS.api_property,
                        Consumed.with(avroSerdes.DfhApiPropertyKey(), avroSerdes.DfhApiPropertyValue()));

        /* STREAM PROCESSORS */
        // 2)
        KTable<dev.data_for_history.api_property.Key, ProfileProperty> apiPropertyProjected = apiPropertyTable
                .mapValues((readOnlyKey, value) -> ProfileProperty.newBuilder()
                        .setProfileId(value.getDfhFkProfile())
                        .setPropertyId(value.getDfhPkProperty())
                        .setDomainId(value.getDfhPropertyDomain())
                        .setRangeId(value.getDfhPropertyRange())
                        .setDeleted$1(Objects.equals(value.getDeleted$1(), "true"))
                        .build()
                );

        // 3) GroupBy
        KGroupedTable<Integer, ProfileProperty> propertyByProfileIdGrouped = apiPropertyProjected
                .groupBy(
                        (key, value) -> KeyValue.pair(value.getProfileId(), value),
                        Grouped.with(
                                Serdes.Integer(), avroSerdes.ProfilePropertyValue()
                        ));
        // 3) Aggregate
        var propertyByProfileIdAggregated = propertyByProfileIdGrouped.aggregate(
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


        // 4)
        var projectProfileTable = projectProfile
                .toTable(
                        Materialized.with(avroSerdes.ProjectProfileKey(), avroSerdes.ProjectProfileValue())
                );

        // 5)
        var projectPropertiesPerProfile = projectProfileTable.join(
                propertyByProfileIdAggregated,
                ProjectProfileValue::getProfileId,
                (projectProfileValue, profilePropertyMap) -> {
                    var projectProperyMap = ProjectPropertyMap.newBuilder().build();
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
                                projectProperyMap.getMap().put(key, v);
                            });
                    return projectProperyMap;
                }
        );

// 3)

        var projectPropertyFlat = projectPropertiesPerProfile
                .toStream(
                        Named.as(inner.TOPICS.project_properties_stream)
                )
                .flatMap((key, value) -> value.getMap().values().stream().map(projectPropertyValue -> {
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
        public final String profile_with_properties = "profile_with_properties";
        public final String project_properties_stream = "project_properties_stream";
        public final String project_properties_flat = "project_properties_flat";
    }

    public enum output {
        TOPICS;
        public final String project_property = Utils.tsPrefixed("project_property");
    }

}
