package org.geovistory.toolbox.streams.base.config.processors;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.base.config.*;
import org.geovistory.toolbox.streams.lib.IdenticalRecordsFilterSupplier;
import org.geovistory.toolbox.streams.lib.Utils;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

@ApplicationScoped
public class ProjectClassLabel {
    @Inject
    AvroSerdes avroSerdes;

    @Inject
    RegisterInputTopic registerInputTopic;

    @Inject
    RegisterInnerTopic registerInnerTopic;

    @Inject
    OutputTopicNames outputTopicNames;

    public ProjectClassLabel(AvroSerdes avroSerdes, RegisterInputTopic registerInputTopic, RegisterInnerTopic registerInnerTopic, OutputTopicNames outputTopicNames) {
        this.avroSerdes = avroSerdes;
        this.registerInputTopic = registerInputTopic;
        this.registerInnerTopic = registerInnerTopic;
        this.outputTopicNames = outputTopicNames;
    }

    public void addProcessorsStandalone() {
        addProcessors(
                registerInputTopic.proProjectTable(),
                registerInputTopic.ontomeClassLabelStream(),
                registerInnerTopic.geovClassLabelStream(),
                registerInnerTopic.projectClassStream()
        );
    }

    public ProjectClassLabelReturnValue addProcessors(
            KTable<dev.projects.project.Key, dev.projects.project.Value> proProjectTable,
            KStream<OntomeClassLabelKey, OntomeClassLabelValue> ontomeClassLabelStream,
            KStream<GeovClassLabelKey, GeovClassLabelValue> geovClassLabelStream,
            KStream<ProjectClassKey, ProjectClassValue> projectClassStream
    ) {

        /* SOURCE PROCESSORS */
        var projectClassTable = projectClassStream.toTable(
                Named.as("project_class_table"),
                Materialized.with(avroSerdes.ProjectClassKey(), avroSerdes.ProjectClassValue())
        );
        var ontomeClassLabelTable = ontomeClassLabelStream.toTable(
                Named.as("ontome_class_label_table"),
                Materialized.with(avroSerdes.OntomeClassLabelKey(), avroSerdes.OntomeClassLabelValue())
        );
        var geovClassLabelTable = geovClassLabelStream.toTable(
                Named.as("geov_class_label_table"),
                Materialized.with(avroSerdes.GeovClassLabelKey(), avroSerdes.GeovClassLabelValue())
        );

        /* STREAM PROCESSORS */
        // 2)
        var projectClassLanguage = projectClassTable.join(
                proProjectTable,
                projectClassValue -> dev.projects.project.Key.newBuilder().setPkEntity(projectClassValue.getProjectId()).build(),
                (value1, value2) -> ProjectClassLanguageValue.newBuilder()
                        .setClassId(value1.getClassId())
                        .setProjectId(value1.getProjectId())
                        .setLanguageId(value2.getFkLanguage())
                        .setDeleted$1(Boolean.TRUE.equals(value1.getDeleted$1()) || Objects.equals(value2.getDeleted$1(), "true"))
                        .build(),
                TableJoined.as("project_class_language" + "-fk-join"),
                Materialized.<ProjectClassKey, ProjectClassLanguageValue, KeyValueStore<Bytes, byte[]>>as("project_class_language")
                        .withKeySerde(avroSerdes.ProjectClassKey())
                        .withValueSerde(avroSerdes.ProjectClassLanguageValue())
        );
        // 3
        var projectClassLabelOptionsTable = projectClassLanguage
                .toStream(
                        Named.as("project_class_language" + "-to-stream")
                )
                .flatMap(
                        (key, value) -> {
                            List<KeyValue<ProjectClassLanguageKey, ProjectClassLanguageValue>> result = new LinkedList<>();

                            // add record for project's language
                            var kProjectLang = ProjectClassLanguageKey.newBuilder()
                                    .setClassId(value.getClassId())
                                    .setProjectId(value.getProjectId())
                                    .setLanguageId(value.getLanguageId())
                                    .build();
                            var vProjectLang = ProjectClassLanguageValue.newBuilder()
                                    .setClassId(value.getClassId())
                                    .setProjectId(value.getProjectId())
                                    .setLanguageId(value.getLanguageId())
                                    .setDeleted$1(value.getDeleted$1())
                                    .build();
                            result.add(KeyValue.pair(kProjectLang, vProjectLang));

                            // if language is not english (18889) add english record
                            if (value.getLanguageId() != 18889) {
                                // add record for english
                                var kEnglish = ProjectClassLanguageKey.newBuilder()
                                        .setClassId(value.getClassId())
                                        .setProjectId(value.getProjectId())
                                        .setLanguageId(18889)
                                        .build();
                                var vEnglish = ProjectClassLanguageValue.newBuilder()
                                        .setClassId(value.getClassId())
                                        .setProjectId(value.getProjectId())
                                        .setLanguageId(18889)
                                        .setDeleted$1(value.getDeleted$1())
                                        .build();
                                result.add(KeyValue.pair(kEnglish, vEnglish));
                            }

                            return result;
                        },
                        Named.as("kstream-flatmap-project-class-language-to-project-class-lang-and-english")
                )
                .toTable(
                        Named.as(inner.TOPICS.project_class_label_options),
                        Materialized
                                .<ProjectClassLanguageKey, ProjectClassLanguageValue, KeyValueStore<Bytes, byte[]>>
                                        as(inner.TOPICS.project_class_label_options + "-store")
                                .withKeySerde(avroSerdes.ProjectClassLanguageKey())
                                .withValueSerde(avroSerdes.ProjectClassLanguageValue())
                );
// 4) left join
        var withGeov = projectClassLabelOptionsTable.leftJoin(
                geovClassLabelTable,
                projectClassLanguageValue -> GeovClassLabelKey.newBuilder()
                        .setClassId(projectClassLanguageValue.getClassId())
                        .setProjectId(projectClassLanguageValue.getProjectId())
                        .setLanguageId(projectClassLanguageValue.getLanguageId())
                        .build(),
                (value1, value2) -> {
                    var result = ProjectClassLabelOptionMap.newBuilder()
                            .setClassId(value1.getClassId())
                            .setProjectId(value1.getProjectId())
                            .setLanguageId(value1.getLanguageId())
                            .build();

                    if (value2 != null) {
                        var isEnglish = value2.getLanguageId() == 18889 ? "en" : "noten";
                        var key = isEnglish + "_" + LabelSource.GEOV_PROJECT;
                        var option = ProjectClassLabelOption.newBuilder()
                                .setClassId(value1.getClassId())
                                .setProjectId(value1.getProjectId())
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
                TableJoined.as("project_class_label_options_with_geov" + "-fk-left-join"),
                Materialized.<ProjectClassLanguageKey, ProjectClassLabelOptionMap, KeyValueStore<Bytes, byte[]>>as("project_class_label_options_with_geov")
                        .withKeySerde(avroSerdes.ProjectClassLanguageKey())
                        .withValueSerde(avroSerdes.ProjectClassLabelOptionMapValue())
        );

// 4b) left join geov default
        var withGeovDefault = withGeov.leftJoin(
                geovClassLabelTable,
                projectClassLanguageValue -> GeovClassLabelKey.newBuilder()
                        .setClassId(projectClassLanguageValue.getClassId())
                        .setProjectId(I.DEFAULT_PROJECT.get())
                        .setLanguageId(projectClassLanguageValue.getLanguageId())
                        .build(),
                (value1, value2) -> {
                    if (value2 != null) {
                        var isEnglish = value2.getLanguageId() == 18889 ? "en" : "noten";
                        var key = isEnglish + "_" + LabelSource.GEOV_DEFAULT_PROJECT;
                        value1.getMap().put(
                                key,
                                ProjectClassLabelOption.newBuilder()
                                        .setClassId(value1.getClassId())
                                        .setProjectId(value1.getProjectId())
                                        .setLanguageId(value1.getLanguageId())
                                        .setSource(LabelSource.GEOV_DEFAULT_PROJECT)
                                        .setDeleted$1(value2.getDeleted$1())
                                        .setLabel(value2.getLabel())
                                        .build()
                        );
                    }
                    return value1;
                },
                TableJoined.as("project_class_label_options_with_geov_and_default" + "-fk-left-join"),
                Materialized.<ProjectClassLanguageKey, ProjectClassLabelOptionMap, KeyValueStore<Bytes, byte[]>>as("project_class_label_options_with_geov_and_default")
                        .withKeySerde(avroSerdes.ProjectClassLanguageKey())
                        .withValueSerde(avroSerdes.ProjectClassLabelOptionMapValue())
        );

        // 5) left join
        var withGeovAndOntome = withGeovDefault.leftJoin(
                ontomeClassLabelTable,
                projectClassLanguageValue -> OntomeClassLabelKey.newBuilder()
                        .setClassId(projectClassLanguageValue.getClassId())
                        .setLanguageId(projectClassLanguageValue.getLanguageId())
                        .build(),
                (value1, value2) -> {
                    if (value2 != null) {
                        var isEnglish = value2.getLanguageId() == 18889 ? "en" : "noten";
                        var key = isEnglish + "_" + LabelSource.ONTOME;
                        value1.getMap().put(
                                key,
                                ProjectClassLabelOption.newBuilder()
                                        .setClassId(value1.getClassId())
                                        .setProjectId(value1.getProjectId())
                                        .setLanguageId(value1.getLanguageId())
                                        .setSource(LabelSource.ONTOME)
                                        .setDeleted$1(value2.getDeleted$1())
                                        .setLabel(value2.getLabel())
                                        .build()
                        );
                    }
                    return value1;
                },
                TableJoined.as("project_class_label_options_with_geov_and_default_and_ontome" + "-fk-left-join"),
                Materialized.<ProjectClassLanguageKey, ProjectClassLabelOptionMap, KeyValueStore<Bytes, byte[]>>as("project_class_label_options_with_geov_and_default_and_ontome")
                        .withKeySerde(avroSerdes.ProjectClassLanguageKey())
                        .withValueSerde(avroSerdes.ProjectClassLabelOptionMapValue())
        );


        // 6) group by
        var projectClassLabelOptionsGrouped = withGeovAndOntome
                .toStream(
                        Named.as("project_class_label_options_with_geov_and_default_and_ontome" + "-to-stream")
                )
                .groupBy(
                        (key, value) ->
                                ProjectClassLabelKey.newBuilder()
                                        .setProjectId(key.getProjectId())
                                        .setClassId(key.getClassId())
                                        .build(),
                        Grouped.with(
                                inner.TOPICS.project_class_label_options_grouped,
                                avroSerdes.ProjectClassLabelKey(), avroSerdes.ProjectClassLabelOptionMapValue()
                        ));

        // 7) aggregate
        var projectClassLabelOptionsAggregated = projectClassLabelOptionsGrouped.aggregate(
                () -> ProjectClassLabelOptionMap.newBuilder()
                        .setClassId(0)
                        .setProjectId(0)
                        .setLanguageId(0)
                        .build(),
                (aggKey, newValue, aggValue) -> {
                    aggValue.setClassId(newValue.getClassId());
                    aggValue.setProjectId(newValue.getProjectId());
                    aggValue.setLanguageId(newValue.getLanguageId());
                    aggValue.getMap().putAll(newValue.getMap());
                    return aggValue;
                },
                Named.as(inner.TOPICS.project_class_label_options_aggregated),
                Materialized.<ProjectClassLabelKey, ProjectClassLabelOptionMap, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.project_class_label_options_aggregated)
                        .withKeySerde(avroSerdes.ProjectClassLabelKey())
                        .withValueSerde(avroSerdes.ProjectClassLabelOptionMapValue())
        );
        var projectClassLabelTable = projectClassLabelOptionsAggregated.mapValues(
                (readOnlyKey, map) -> {
                    ProjectClassLabelValue o;
                    // Label in project language, provided by Geovistory project
                    o = toValue(map, "noten_" + LabelSource.GEOV_PROJECT);
                    if (o != null) return o;

                    // Label in project language, provided by Geovistory default project
                    o = toValue(map, "noten_" + LabelSource.GEOV_DEFAULT_PROJECT);
                    if (o != null) return o;

                    // Label in project language, provided by OntoME
                    o = toValue(map, "noten_" + LabelSource.ONTOME);
                    if (o != null) return o;

                    // Label in english, provided by Geovistory project
                    o = toValue(map, "en_" + LabelSource.GEOV_PROJECT);
                    if (o != null) return o;

                    // Label in english, provided by Geovistory default project
                    o = toValue(map, "en_" + LabelSource.GEOV_DEFAULT_PROJECT);
                    if (o != null) return o;

                    // Label in english, provided by OntoME
                    o = toValue(map, "en_" + LabelSource.ONTOME);
                    if (o != null) return o;

                    return o;
                },
                Named.as("ktable-mapvalues-project-class-label")
        );

        var projectClassLabelStream = projectClassLabelTable
                .toStream(Named.as("project_class_label" + "-to-stream"))
                .transform(new IdenticalRecordsFilterSupplier<>(
                        "project_class_label_identical_records_filter",
                        avroSerdes.ProjectClassLabelKey(),
                        avroSerdes.ProjectClassLabelValue()
                ));

        /* SINK PROCESSORS */

        // 8) to
        projectClassLabelStream.to(outputTopicNames.projectClassLabel(),
                Produced.with(avroSerdes.ProjectClassLabelKey(), avroSerdes.ProjectClassLabelValue())
                        .withName(outputTopicNames.projectClassLabel() + "-producer")
        );

        return new ProjectClassLabelReturnValue(projectClassLabelTable, projectClassLabelStream);

    }

    /**
     * pick key from ProjectClassLabelOptionMap and
     * convert ProjectClassLabelOption to ProjectClassLabelValue
     * if key is not available return null
     *
     * @param map ProjectClassLabelOptionMap
     * @param key String
     * @return ProjectClassLabelValue
     */
    private static ProjectClassLabelValue toValue(ProjectClassLabelOptionMap map, String key) {
        var val = map.getMap().get(key);
        if (val != null && !Boolean.TRUE.equals(val.getDeleted$1())) return ProjectClassLabelValue.newBuilder()
                .setProjectId(val.getProjectId())
                .setClassId(val.getClassId())
                .setLanguageId(val.getLanguageId())
                .setLanguageIso(val.getLanguageIso())
                .setLabel(val.getLabel())
                .setDeleted$1(val.getDeleted$1())
                .build();
        return null;
    }


    public enum inner {
        TOPICS;
        public final String project_class_label_options = "project_class_label_options";
        public final String project_class_label_options_grouped = "project_class_label_options_grouped";
        public final String project_class_label_options_aggregated = "project_class_label_options_aggregated";

    }

}
