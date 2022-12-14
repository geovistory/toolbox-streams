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


public class ProjectClass {

    public static void main(String[] args) {
        System.out.println(buildStandalone(new StreamsBuilder()).describe());
    }

    public static Topology buildStandalone(StreamsBuilder builder) {

        var registerInputTopic = new RegisterInputTopic(builder);
        var registerOutputTopic = new RegisterOutputTopic(builder);


        var apiClassTable = registerInputTopic.dfhApiClassTable();
        var projectProfileStream = registerOutputTopic.projectProfileStream();

        return addProcessors(builder, projectProfileStream, apiClassTable).builder().build();
    }

    public static ProjectClassReturnValue addProcessors(
            StreamsBuilder builder,
            KStream<ProjectProfileKey, ProjectProfileValue> projectProfileStream,
            KTable<dev.data_for_history.api_class.Key, dev.data_for_history.api_class.Value> apiClassTable
    ) {

        var avroSerdes = new ConfluentAvroSerdes();

        /* STREAM PROCESSORS */
        // 2)
        KTable<dev.data_for_history.api_class.Key, ProfileClass> apiClassProjected = apiClassTable
                .mapValues((readOnlyKey, value) -> ProfileClass.newBuilder()
                        .setProfileId(value.getDfhFkProfile())
                        .setClassId(value.getDfhPkClass())
                        .setDeleted$1(Objects.equals(value.getDeleted$1(), "true"))
                        .build()
                );

        // 3) GroupBy
        KGroupedTable<Integer, ProfileClass> classByProfileIdGrouped = apiClassProjected
                .groupBy(
                        (key, value) -> KeyValue.pair(value.getProfileId(), value),
                        Grouped.with(
                                Serdes.Integer(), avroSerdes.ProfileClassValue()
                        ));
        // 3) Aggregate
        var classByProfileIdAggregated = classByProfileIdGrouped.aggregate(
                () -> ProfileClassMap.newBuilder().build(),
                (aggKey, newValue, aggValue) -> {
                    var key = newValue.getProfileId() + "_" + newValue.getClassId();
                    aggValue.getMap().put(key, newValue);
                    return aggValue;
                },
                (aggKey, oldValue, aggValue) -> aggValue,
                Named.as(inner.TOPICS.profile_with_classes)
                ,
                Materialized.<Integer, ProfileClassMap, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.profile_with_classes)
                        .withKeySerde(Serdes.Integer())
                        .withValueSerde(avroSerdes.ProfileClassMapValue())
        );


        // 4)
        var projectProfileTable = projectProfileStream
                .toTable(
                        Materialized.with(avroSerdes.ProjectProfileKey(), avroSerdes.ProjectProfileValue())
                );

        // 5)
        var projectPropertiesPerProfile = projectProfileTable.join(
                classByProfileIdAggregated,
                ProjectProfileValue::getProfileId,
                (projectProfileValue, profileClassMap) -> {
                    var projectProperyMap = ProjectClassMap.newBuilder().build();
                    profileClassMap.getMap().values()
                            .forEach(apiClass -> {
                                var projectId = projectProfileValue.getProjectId();
                                var projectClassIsDeleted = projectProfileValue.getDeleted$1() || apiClass.getDeleted$1();

                                var v = ProjectClassValue.newBuilder()
                                        .setProjectId(projectId)
                                        .setClassId(apiClass.getClassId())
                                        .setDeleted$1(projectClassIsDeleted)
                                        .build();
                                var key = projectId + "_" + apiClass.getClassId();
                                // ... and add one project-class
                                projectProperyMap.getMap().put(key, v);
                            });
                    return projectProperyMap;
                }
        );

// 3)

        var projectClassFlat = projectPropertiesPerProfile
                .toStream(
                        Named.as(inner.TOPICS.project_classes_stream)
                )
                .flatMap((key, value) -> value.getMap().values().stream().map(projectClassValue -> {
                                    var k = ProjectClassKey.newBuilder()
                                            .setClassId(projectClassValue.getClassId())
                                            .setProjectId(projectClassValue.getProjectId())
                                            .build();
                                    return KeyValue.pair(k, projectClassValue);
                                }
                        ).toList(),
                        Named.as(inner.TOPICS.project_classes_flat));

        projectClassFlat.to(output.TOPICS.project_class,
                Produced.with(avroSerdes.ProjectClassKey(), avroSerdes.ProjectClassValue()));

        return new ProjectClassReturnValue(builder, projectClassFlat);

    }


    public enum input {
        TOPICS;
        public final String project_profile = ProjectProfiles.output.TOPICS.project_profile;
        public final String api_class = DbTopicNames.dfh_api_class.getName();
    }


    public enum inner {
        TOPICS;
        public final String profile_with_classes = "profile_with_classes";
        public final String project_classes_stream = "project_classes_stream";
        public final String project_classes_flat = "project_classes_flat";
    }

    public enum output {
        TOPICS;
        public final String project_class = Utils.tsPrefixed("project_class");
    }

}
