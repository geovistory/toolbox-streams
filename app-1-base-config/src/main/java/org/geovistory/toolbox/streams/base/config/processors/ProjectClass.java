package org.geovistory.toolbox.streams.base.config.processors;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.base.config.AvroSerdes;
import org.geovistory.toolbox.streams.base.config.OutputTopicNames;
import org.geovistory.toolbox.streams.base.config.RegisterInnerTopic;
import org.geovistory.toolbox.streams.base.config.RegisterInputTopic;
import org.geovistory.toolbox.streams.lib.IdenticalRecordsFilterSupplier;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.LinkedList;
import java.util.Objects;

@ApplicationScoped
public class ProjectClass {
    @Inject
    AvroSerdes avroSerdes;

    @Inject
    RegisterInputTopic registerInputTopic;

    @Inject
    RegisterInnerTopic registerInnerTopic;

    @Inject
    OutputTopicNames outputTopicNames;

    public ProjectClass(AvroSerdes avroSerdes, RegisterInputTopic registerInputTopic, RegisterInnerTopic registerInnerTopic, OutputTopicNames outputTopicNames) {
        this.avroSerdes = avroSerdes;
        this.registerInputTopic = registerInputTopic;
        this.registerInnerTopic = registerInnerTopic;
        this.outputTopicNames = outputTopicNames;
    }

    public void addProcessorsStandalone() {
        var projectProfileStream = registerInnerTopic.projectProfileStream();
        var ontomeClassStream = registerInputTopic.ontomeClassStream();

        addProcessors(projectProfileStream, ontomeClassStream);
    }

    public ProjectClassReturnValue addProcessors(
            KStream<ProjectProfileKey, ProjectProfileValue> projectProfileStream,
            KStream<OntomeClassKey, OntomeClassValue> ontomeClassStream
    ) {
        /* STREAM PROCESSORS */
        // 2)
        var ontomeClassProjected = ontomeClassStream
                .mapValues(
                        (readOnlyKey, value) -> ProfileClass.newBuilder()
                                .setProfileId(value.getDfhFkProfile())
                                .setClassId(value.getDfhPkClass())
                                .setDeleted$1(Objects.equals(value.getDeleted$1(), "true"))
                                .build(),
                        Named.as("kstream-mapvalues-ontome-class-to-profile-class")
                );

        // 3) GroupBy
        var classByProfileIdGrouped = ontomeClassProjected
                .groupBy(
                        (key, value) -> value.getProfileId(),
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
                Named.as(inner.TOPICS.profile_with_classes)
                ,
                Materialized.<Integer, ProfileClassMap, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.profile_with_classes)
                        .withKeySerde(Serdes.Integer())
                        .withValueSerde(avroSerdes.ProfileClassMapValue())
        );


        // 4)
        var projectProfileTable = projectProfileStream
                .toTable(
                        Named.as(inner.TOPICS.profile_with_classes + "-to-table"),
                        Materialized
                                .<ProjectProfileKey, ProjectProfileValue, KeyValueStore<Bytes, byte[]>>
                                        as(inner.TOPICS.profile_with_classes + "-store")
                                .withKeySerde(avroSerdes.ProjectProfileKey())
                                .withValueSerde(avroSerdes.ProjectProfileValue())
                );

        // 5)
        var projectClassPerProfile = projectProfileTable.join(
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
                },
                TableJoined.as("project_classes_per_profile" + "-fk-join")
        );

// 3)

        var projectClassFlat = projectClassPerProfile
                .toStream(
                        Named.as(inner.TOPICS.project_classes_stream + "-to-stream")
                )
                .flatMap((key, value) -> {
                            try {
                                return value.getMap().values().stream().map(projectClassValue -> {
                                            var k = ProjectClassKey.newBuilder()
                                                    .setClassId(projectClassValue.getClassId())
                                                    .setProjectId(projectClassValue.getProjectId())
                                                    .build();
                                            return KeyValue.pair(k, projectClassValue);
                                        }
                                ).toList();
                            } catch (RuntimeException e) {
                                return new LinkedList<>();
                            }
                        },
                        Named.as(inner.TOPICS.project_classes_flat))
                .transform(new IdenticalRecordsFilterSupplier<>(
                                "project_class_suppress_duplicates",
                                avroSerdes.ProjectClassKey(),
                                avroSerdes.ProjectClassValue()
                        ),
                        Named.as("project_class_suppress_duplicates"));

        projectClassFlat
                .to(outputTopicNames.projectClass(),
                        Produced.with(avroSerdes.ProjectClassKey(), avroSerdes.ProjectClassValue())
                                .withName(outputTopicNames.projectClass() + "-producer")
                );

        return new ProjectClassReturnValue(projectClassFlat);

    }


    public enum inner {
        TOPICS;
        public final String profile_with_classes = "profile_with_classes";
        public final String project_classes_stream = "project_classes_stream";
        public final String project_classes_flat = "project_classes_flat";
    }



}
