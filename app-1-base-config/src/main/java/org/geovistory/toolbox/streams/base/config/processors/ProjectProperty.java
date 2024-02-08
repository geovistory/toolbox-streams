package org.geovistory.toolbox.streams.base.config.processors;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.base.config.OutputTopicNames;
import org.geovistory.toolbox.streams.base.config.RegisterInnerTopic;
import org.geovistory.toolbox.streams.base.config.RegisterInputTopic;
import org.geovistory.toolbox.streams.lib.ConfiguredAvroSerde;
import org.geovistory.toolbox.streams.lib.IdenticalRecordsFilterSupplier;

import java.util.LinkedList;
import java.util.Objects;


@ApplicationScoped
public class ProjectProperty {
    @Inject
    ConfiguredAvroSerde as;

    @Inject
    RegisterInputTopic registerInputTopic;

    @Inject
    RegisterInnerTopic registerInnerTopic;

    @Inject
    OutputTopicNames outputTopicNames;

    public ProjectProperty(ConfiguredAvroSerde as, RegisterInputTopic registerInputTopic, RegisterInnerTopic registerInnerTopic, OutputTopicNames outputTopicNames) {
        this.as = as;
        this.registerInputTopic = registerInputTopic;
        this.registerInnerTopic = registerInnerTopic;
        this.outputTopicNames = outputTopicNames;
    }

    public void addProcessorsStandalone() {

        addProcessors(
                registerInputTopic.ontomePropertyStream(),
                registerInnerTopic.projectProfileStream()
        );
    }

    public ProjectPropertyReturnValue addProcessors(
            KStream<OntomePropertyKey, OntomePropertyValue> ontomePropertyStream,
            KStream<ProjectProfileKey, ProjectProfileValue> projectProfileStream) {

        /* STREAM PROCESSORS */

        // 3) GroupBy
        var propertyByProfileIdGrouped = ontomePropertyStream
                .groupBy(
                        (key, value) -> value.getDfhFkProfile(),
                        Grouped.with(
                                Serdes.Integer(), as.value()
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
                        .withValueSerde(as.value())
        );


        // 4)
        var projectProfileTable = projectProfileStream
                .toTable(
                        Named.as(inner.TOPICS.project_profile + "-to-table"),
                        Materialized
                                .<ProjectProfileKey, ProjectProfileValue, KeyValueStore<Bytes, byte[]>>
                                        as(inner.TOPICS.project_profile + "-store")
                                .withKeySerde(as.key())
                                .withValueSerde(as.value())
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
                TableJoined.as("project_properties_per_profile" + "-fk-join")
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
                        Named.as(inner.TOPICS.project_properties_flat))
                .transform(new IdenticalRecordsFilterSupplier<>("project_property_suppress_duplicates", as.key(), as.value()),
                        Named.as("project_property_suppress_duplicates"));

        projectPropertyStream.to(outputTopicNames.projectProperty(),
                Produced.with(as.<ProjectPropertyKey>key(), as.<ProjectPropertyValue>value())
                        .withName(outputTopicNames.projectProperty() + "-producer")
        );

        return new ProjectPropertyReturnValue(projectPropertyStream);

    }


    public enum inner {
        TOPICS;
        public final String profile_with_properties = "profile_with_properties";
        public final String project_properties_stream = "project_properties_stream";
        public final String project_properties_flat = "project_properties_flat";
        public final String project_profile = "project_profile";
    }


}
