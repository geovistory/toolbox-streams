package org.geovistory.toolbox.streams.topologies;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
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

import java.util.Objects;


public class ProjectProperty {

    public static void main(String[] args) {
        System.out.println(buildStandalone(new StreamsBuilder()).describe());
    }

    public static Topology buildStandalone(StreamsBuilder builder) {
        var registerInputTopic = new RegisterInputTopic(builder);
        var registerOutputTopic = new RegisterOutputTopic(builder);

        return addProcessors(
                builder,
                registerInputTopic.dfhApiPropertyTable(),
                registerOutputTopic.projectProfileStream()
        ).build();
    }

    public static StreamsBuilder addProcessors(
            StreamsBuilder builder,
            KTable<dev.data_for_history.api_property.Key, dev.data_for_history.api_property.Value> dfhApiPropertyTable,
            KStream<ProjectProfileKey, ProjectProfileValue> projectProfileStream) {

        var avroSerdes = new ConfluentAvroSerdes();


        /* STREAM PROCESSORS */
        // 2)
        KTable<dev.data_for_history.api_property.Key, ProfileProperty> apiPropertyProjected = dfhApiPropertyTable
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
        var projectProfileTable = projectProfileStream
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
        public final String project_profile = ProjectProfiles.output.TOPICS.project_profile;
        public final String api_property = DbTopicNames.dfh_api_property.getName();
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
