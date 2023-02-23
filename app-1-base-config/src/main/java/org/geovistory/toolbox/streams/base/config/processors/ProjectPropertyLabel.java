package org.geovistory.toolbox.streams.base.config.processors;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.base.config.*;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.Utils;
import org.geovistory.toolbox.streams.project.config.*;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;


public class ProjectPropertyLabel {

    public static void main(String[] args) {
        System.out.println(buildStandalone(new StreamsBuilder()).describe());
    }

    public static Topology buildStandalone(StreamsBuilder builder) {
        var registerInputTopic = new RegisterInputTopic(builder);
        var registerInnerTopic = new RegisterInnerTopic(builder);

        return addProcessors(
                builder,
                registerInputTopic.proProjectTable(),
                registerInputTopic.ontomePropertyLabelStream(),
                registerInnerTopic.geovPropertyLabelStream(),
                registerInnerTopic.projectPropertyStream()
        ).builder().build();
    }

    public static ProjectPropertyLabelReturnValue addProcessors(
            StreamsBuilder builder,
            KTable<dev.projects.project.Key, dev.projects.project.Value> proProjectTable,
            KStream<OntomePropertyLabelKey, OntomePropertyLabelValue> ontomePropertyLabelStream,
            KStream<GeovPropertyLabelKey, GeovPropertyLabelValue> geovPropertyLabelStream,
            KStream<ProjectPropertyKey, ProjectPropertyValue> projectPropertyStream
    ) {

        var avroSerdes = new ConfluentAvroSerdes();

        /* SOURCE PROCESSORS */
        var projectPropertyTable = projectPropertyStream.toTable(
                Named.as("project_property_table"),
                Materialized.<ProjectPropertyKey, ProjectPropertyValue, KeyValueStore<Bytes, byte[]>>
                                as("project_property_table" + "-store")
                        .withKeySerde(avroSerdes.ProjectPropertyKey())
                        .withValueSerde(avroSerdes.ProjectPropertyValue())
        );
        var ontomePropertyLabelTable = ontomePropertyLabelStream.toTable(
                Named.as("ontome_property_label_table"),
                Materialized.<OntomePropertyLabelKey, OntomePropertyLabelValue, KeyValueStore<Bytes, byte[]>>
                                as("ontome_property_label_table" + "-store")
                        .withKeySerde(avroSerdes.OntomePropertyLabelKey())
                        .withValueSerde(avroSerdes.OntomePropertyLabelValue())
        );
        var geovPropertyLabelTable = geovPropertyLabelStream.toTable(
                Named.as("geov_property_label_table"),
                Materialized.<GeovPropertyLabelKey, GeovPropertyLabelValue, KeyValueStore<Bytes, byte[]>>
                                as("geov_property_label_table" + "-store")
                        .withKeySerde(avroSerdes.GeovPropertyLabelKey())
                        .withValueSerde(avroSerdes.GeovPropertyLabelValue())
        );

        /* STREAM PROCESSORS */
        // 2)
        var projectPropertyLanguage = projectPropertyTable.join(
                proProjectTable,
                projectPropertyValue -> dev.projects.project.Key.newBuilder().setPkEntity(projectPropertyValue.getProjectId()).build(),
                (value1, value2) -> ProjectPropertyLanguageValue.newBuilder()
                        .setProjectId(value1.getProjectId())
                        .setDomainId(value1.getDomainId())
                        .setPropertyId(value1.getPropertyId())
                        .setRangeId(value1.getRangeId())
                        .setLanguageId(value2.getFkLanguage())
                        .setDeleted$1(Boolean.TRUE.equals(value1.getDeleted$1()) || Objects.equals(value2.getDeleted$1(), "true"))
                        .build(),
                TableJoined.as("project_property_language" + "-fk-join"),
                Materialized.<ProjectPropertyKey, ProjectPropertyLanguageValue, KeyValueStore<Bytes, byte[]>>as("project_property_language")
                        .withKeySerde(avroSerdes.ProjectPropertyKey())
                        .withValueSerde(avroSerdes.ProjectPropertyLanguageValue())
        );
        // 3
        var projectPropertyLabelOptionsTable = projectPropertyLanguage
                .toStream(
                        Named.as("project_property_language" + "-to-stream")
                )
                .flatMap(
                        (key, value) -> {
                            List<KeyValue<ProjectFieldLanguageKey, ProjectFieldLanguageValue>> result = new LinkedList<>();

                            // add record for outgoing in project's language
                            var kProjectLang = ProjectFieldLanguageKey.newBuilder()
                                    .setProjectId(value.getProjectId())
                                    .setClassId(value.getDomainId())
                                    .setPropertyId(value.getPropertyId())
                                    .setIsOutgoing(true)
                                    .setLanguageId(value.getLanguageId())
                                    .build();
                            var vProjectLang = ProjectFieldLanguageValue.newBuilder()
                                    .setProjectId(value.getProjectId())
                                    .setClassId(value.getDomainId())
                                    .setPropertyId(value.getPropertyId())
                                    .setIsOutgoing(true)
                                    .setLanguageId(value.getLanguageId())
                                    .setDeleted$1(value.getDeleted$1())
                                    .build();
                            result.add(KeyValue.pair(kProjectLang, vProjectLang));

                            // add record for incoming in project's language
                            kProjectLang = ProjectFieldLanguageKey.newBuilder()
                                    .setProjectId(value.getProjectId())
                                    .setClassId(value.getRangeId())
                                    .setPropertyId(value.getPropertyId())
                                    .setIsOutgoing(false)
                                    .setLanguageId(value.getLanguageId())
                                    .build();
                            vProjectLang = ProjectFieldLanguageValue.newBuilder()
                                    .setProjectId(value.getProjectId())
                                    .setClassId(value.getRangeId())
                                    .setPropertyId(value.getPropertyId())
                                    .setIsOutgoing(false)
                                    .setLanguageId(value.getLanguageId())
                                    .setDeleted$1(value.getDeleted$1())
                                    .build();
                            result.add(KeyValue.pair(kProjectLang, vProjectLang));

                            // if language is not english (18889) add english record
                            if (value.getLanguageId() != I.EN.get()) {
                                // add record for outgoing in english
                                var kEnglish = ProjectFieldLanguageKey.newBuilder()
                                        .setProjectId(value.getProjectId())
                                        .setClassId(value.getDomainId())
                                        .setPropertyId(value.getPropertyId())
                                        .setIsOutgoing(true)
                                        .setLanguageId(I.EN.get())
                                        .build();
                                var vEnglish = ProjectFieldLanguageValue.newBuilder()
                                        .setProjectId(value.getProjectId())
                                        .setClassId(value.getDomainId())
                                        .setPropertyId(value.getPropertyId())
                                        .setIsOutgoing(true)
                                        .setLanguageId(I.EN.get())
                                        .setDeleted$1(value.getDeleted$1())
                                        .build();
                                result.add(KeyValue.pair(kEnglish, vEnglish));

                                // add record for incoming in english
                                kEnglish = ProjectFieldLanguageKey.newBuilder()
                                        .setProjectId(value.getProjectId())
                                        .setClassId(value.getRangeId())
                                        .setPropertyId(value.getPropertyId())
                                        .setIsOutgoing(false)
                                        .setLanguageId(I.EN.get())
                                        .build();
                                vEnglish = ProjectFieldLanguageValue.newBuilder()
                                        .setProjectId(value.getProjectId())
                                        .setClassId(value.getRangeId())
                                        .setPropertyId(value.getPropertyId())
                                        .setIsOutgoing(false)
                                        .setLanguageId(I.EN.get())
                                        .setDeleted$1(value.getDeleted$1())
                                        .build();
                                result.add(KeyValue.pair(kEnglish, vEnglish));
                            }

                            return result;
                        },
                        Named.as("kstream-project-property-lang-to-project-fields-in-project-lang-and-english")
                )
                .toTable(
                        Named.as(inner.TOPICS.project_property_label_options),
                        Materialized
                                .<ProjectFieldLanguageKey, ProjectFieldLanguageValue, KeyValueStore<Bytes, byte[]>>
                                        as(inner.TOPICS.project_property_label_options + "-store")
                                .withKeySerde(avroSerdes.ProjectPropertyLanguageKey())
                                .withValueSerde(avroSerdes.ProjectFieldLanguageValue())
                );

        // 4.1) left join labels of project
        var withGeovOfProject = projectPropertyLabelOptionsTable.leftJoin(
                geovPropertyLabelTable,
                projectPropertyLanguageValue -> GeovPropertyLabelKey.newBuilder()
                        .setProjectId(projectPropertyLanguageValue.getProjectId())
                        .setClassId(projectPropertyLanguageValue.getClassId())
                        .setPropertyId(projectPropertyLanguageValue.getPropertyId())
                        .setIsOutgoing(projectPropertyLanguageValue.getIsOutgoing())
                        .setLanguageId(projectPropertyLanguageValue.getLanguageId())
                        .build(),
                (value1, value2) -> {
                    var result = ProjectFieldLabelOptionMap.newBuilder()
                            .setProjectId(value1.getProjectId())
                            .setClassId(value1.getClassId())
                            .setPropertyId(value1.getPropertyId())
                            .setIsOutgoing(value1.getIsOutgoing())
                            .setLanguageId(value1.getLanguageId())
                            .build();

                    if (value2 != null) {
                        var isEnglish = value2.getLanguageId() == 18889 ? "en" : "noten";
                        var direction = value2.getIsOutgoing() ? "out" : "in";

                        var key = isEnglish + "_" + direction + "_" + LabelSource.GEOV_PROJECT;
                        var option = ProjectFieldLabelOption.newBuilder()
                                .setProjectId(value1.getProjectId())
                                .setClassId(value1.getClassId())
                                .setPropertyId(value1.getPropertyId())
                                .setIsOutgoing(true)
                                .setLanguageId(value1.getLanguageId())
                                .setSource(LabelSource.GEOV_PROJECT)
                                .setDeleted$1(Utils.includesTrue(value1.getDeleted$1(), value2.getDeleted$1()))
                                .setLabel(value2.getLabel())
                                .build();
                        result.getMap().put(
                                key, option
                        );
                    }
                    return result;
                },
                TableJoined.as("project_property_label_options_with_geov" + "-fk-left-join"),
                Materialized.<ProjectFieldLanguageKey, ProjectFieldLabelOptionMap, KeyValueStore<Bytes, byte[]>>as("project_property_label_options_with_geov")
                        .withKeySerde(avroSerdes.ProjectPropertyLanguageKey())
                        .withValueSerde(avroSerdes.ProjectPropertyLabelOptionMapValue())
        );

        // 4.2) left join labels of geov default
        var withGeov = withGeovOfProject.leftJoin(
                geovPropertyLabelTable,
                projectPropertyLanguageValue -> GeovPropertyLabelKey.newBuilder()
                        .setProjectId(I.DEFAULT_PROJECT.get())
                        .setClassId(projectPropertyLanguageValue.getClassId())
                        .setPropertyId(projectPropertyLanguageValue.getPropertyId())
                        .setIsOutgoing(projectPropertyLanguageValue.getIsOutgoing())
                        .setLanguageId(projectPropertyLanguageValue.getLanguageId())
                        .build(),
                (value1, value2) -> {
                    if (value2 != null) {

                        var isEnglish = value2.getLanguageId() == 18889 ? "en" : "noten";
                        var direction = value2.getIsOutgoing() ? "out" : "in";
                        var key = isEnglish + "_" + direction + "_" + LabelSource.GEOV_DEFAULT_PROJECT;

                        value1.getMap().put(
                                key,
                                ProjectFieldLabelOption.newBuilder()
                                        .setProjectId(value1.getProjectId())
                                        .setClassId(value1.getClassId())
                                        .setPropertyId(value1.getPropertyId())
                                        .setIsOutgoing(value1.getIsOutgoing())
                                        .setLanguageId(value1.getLanguageId())
                                        .setSource(LabelSource.GEOV_DEFAULT_PROJECT)
                                        .setDeleted$1(value2.getDeleted$1())
                                        .setLabel(value2.getLabel())
                                        .build()
                        );
                    }
                    return value1;
                },
                TableJoined.as("project_property_label_options_with_geov_and_default" + "-fk-left-join"),
                Materialized.<ProjectFieldLanguageKey, ProjectFieldLabelOptionMap, KeyValueStore<Bytes, byte[]>>as("project_property_label_options_with_geov_and_default")
                        .withKeySerde(avroSerdes.ProjectPropertyLanguageKey())
                        .withValueSerde(avroSerdes.ProjectPropertyLabelOptionMapValue())
        );


        // 5) left join
        var withGeovAndOntome = withGeov.leftJoin(
                ontomePropertyLabelTable,
                projectPropertyLanguageValue -> OntomePropertyLabelKey.newBuilder()
                        .setPropertyId(projectPropertyLanguageValue.getPropertyId())
                        .setLanguageId(projectPropertyLanguageValue.getLanguageId())
                        .build(),
                (value1, value2) -> {
                    if (value2 != null) {
                        var isEnglish = value2.getLanguageId() == 18889 ? "en" : "noten";
                        var direction = value1.getIsOutgoing() ? "out" : "in";
                        var key = isEnglish + "_" + direction + "_" + LabelSource.ONTOME;
                        var label = value1.getIsOutgoing() ? value2.getLabel() : value2.getInverseLabel();
                        value1.getMap().put(
                                key,
                                ProjectFieldLabelOption.newBuilder()
                                        .setProjectId(value1.getProjectId())
                                        .setClassId(value1.getClassId())
                                        .setPropertyId(value1.getPropertyId())
                                        .setIsOutgoing(value1.getIsOutgoing())
                                        .setLanguageId(value1.getLanguageId())
                                        .setSource(LabelSource.ONTOME)
                                        .setDeleted$1(value2.getDeleted$1())
                                        .setLabel(label)
                                        .build()
                        );
                    }
                    return value1;
                },
                TableJoined.as("project_property_label_options_with_geov_and_default_and_ontome" + "-fk-left-join"),
                Materialized.<ProjectFieldLanguageKey, ProjectFieldLabelOptionMap, KeyValueStore<Bytes, byte[]>>as("project_property_label_options_with_geov_and_default_and_ontome")
                        .withKeySerde(avroSerdes.ProjectPropertyLanguageKey())
                        .withValueSerde(avroSerdes.ProjectPropertyLabelOptionMapValue())
        );

        // 6) group by
        var projectPropertyLabelOptionsGrouped = withGeovAndOntome
                .toStream(
                        Named.as("project_property_label_options_with_geov_and_default_and_ontome" + "-to-stream")
                )
                .groupBy(
                        (key, value) ->
                                ProjectFieldLabelKey.newBuilder()
                                        .setProjectId(key.getProjectId())
                                        .setClassId(value.getClassId())
                                        .setPropertyId(key.getPropertyId())
                                        .setIsOutgoing(value.getIsOutgoing())
                                        .build(),
                        Grouped.with(
                                inner.TOPICS.project_property_label_options_grouped,
                                avroSerdes.ProjectPropertyLabelKey(), avroSerdes.ProjectPropertyLabelOptionMapValue()
                        ));

        // 7) aggregate
        var projectPropertyLabelOptionsAggregated = projectPropertyLabelOptionsGrouped.aggregate(
                () -> ProjectFieldLabelOptionMap.newBuilder()
                        .setProjectId(0)
                        .setClassId(0)
                        .setPropertyId(0)
                        .setIsOutgoing(true)
                        .setLanguageId(0)
                        .build(),
                (aggKey, newValue, aggValue) -> {
                    aggValue.setProjectId(newValue.getProjectId());
                    aggValue.setClassId(newValue.getClassId());
                    aggValue.setPropertyId(newValue.getPropertyId());
                    aggValue.setIsOutgoing(newValue.getIsOutgoing());
                    aggValue.setLanguageId(newValue.getLanguageId());
                    aggValue.getMap().putAll(newValue.getMap());
                    return aggValue;
                },
                Named.as(inner.TOPICS.project_property_label_options_aggregated),
                Materialized.<ProjectFieldLabelKey, ProjectFieldLabelOptionMap, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.project_property_label_options_aggregated)
                        .withKeySerde(avroSerdes.ProjectPropertyLabelKey())
                        .withValueSerde(avroSerdes.ProjectPropertyLabelOptionMapValue())
        );
        var projectPropertyLabelTable = projectPropertyLabelOptionsAggregated.mapValues(
                (readOnlyKey, map) -> {
                    ProjectFieldLabelValue o;
                    var direction = readOnlyKey.getIsOutgoing() ? "out" : "in";
                    // Label in project language, provided by Geovistory project
                    o = toValue(map, "noten_" + direction + "_" + LabelSource.GEOV_PROJECT);
                    if (o != null) return o;

                    // Label in project language, provided by Geovistory default project
                    o = toValue(map, "noten_" + direction + "_" + LabelSource.GEOV_DEFAULT_PROJECT);
                    if (o != null) return o;

                    // Label in project language, provided by OntoME
                    o = toValue(map, "noten_" + direction + "_" + LabelSource.ONTOME);
                    if (o != null) return o;

                    // Label in english, provided by Geovistory project
                    o = toValue(map, "en_" + direction + "_" + LabelSource.GEOV_PROJECT);
                    if (o != null) return o;

                    // Label in english, provided by Geovistory default project
                    o = toValue(map, "en_" + direction + "_" + LabelSource.GEOV_DEFAULT_PROJECT);
                    if (o != null) return o;

                    // Label in english, provided by OntoME
                    o = toValue(map, "en_" + direction + "_" + LabelSource.ONTOME);
                    if (o != null) return o;

                    return o;
                },
                Named.as("ktable-mapvalues-project-property-label")
        );
        var projectPropertyLabelStream = projectPropertyLabelTable.toStream(
                Named.as("project_property_label_table" + "-to-stream")
        );

        /* SINK PROCESSORS */

        // 8) to
        projectPropertyLabelStream.to(output.TOPICS.project_property_label,
                Produced.with(avroSerdes.ProjectPropertyLabelKey(), avroSerdes.ProjectPropertyLabelValue())
                        .withName(output.TOPICS.project_property_label + "-producer")
        );

        return new ProjectPropertyLabelReturnValue(builder, projectPropertyLabelTable, projectPropertyLabelStream);

    }

    /**
     * pick key from ProjectPropertyLabelOptionMap and
     * convert ProjectPropertyLabelOption to ProjectPropertyLabelValue
     * if key is not available return null
     *
     * @param map ProjectPropertyLabelOptionMap
     * @param key String
     * @return ProjectPropertyLabelValue
     */
    private static ProjectFieldLabelValue toValue(ProjectFieldLabelOptionMap map, String key) {
        var val = map.getMap().get(key);
        if (val != null && !Boolean.TRUE.equals(val.getDeleted$1())) return ProjectFieldLabelValue.newBuilder()
                .setProjectId(val.getProjectId())
                .setClassId(val.getClassId())
                .setPropertyId(val.getPropertyId())
                .setIsOutgoing(val.getIsOutgoing())
                .setLanguageId(val.getLanguageId())
                .setLanguageIso(val.getLanguageIso())
                .setLabel(val.getLabel())
                .setDeleted$1(val.getDeleted$1())
                .build();
        return null;
    }

    public enum input {
        TOPICS;
        public final String ontome_property_label = Env.INSTANCE.TOPIC_ONTOME_PROPERTY_LABEL;
        public final String geov_property_label = GeovPropertyLabel.output.TOPICS.geov_property_label;
        public final String project_property = ProjectProperty.output.TOPICS.project_property;

        public final String project = DbTopicNames.pro_projects.getName();
    }


    public enum inner {
        TOPICS;
        public final String project_property_label_options = "project_property_label_options";
        public final String project_property_label_options_grouped = "project_property_label_options_grouped";
        public final String project_property_label_options_aggregated = "project_property_label_options_aggregated";

    }

    public enum output {
        TOPICS;
        public final String project_property_label = Utils.tsPrefixed("project_property_label");
    }

}
