package org.geovistory.toolbox.streams.topologies;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.Utils;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;


public class ProjectClassLabel {

    public static void main(String[] args) {
        System.out.println(buildStandalone(new StreamsBuilder()).describe());
    }

    public static Topology buildStandalone(StreamsBuilder builder) {
        var avroSerdes = new ConfluentAvroSerdes();
        // 1)
        var ontomeClassLabelStream = builder
                .stream(input.TOPICS.ontome_class_label,
                        Consumed.with(avroSerdes.OntomeClassLabelKey(), avroSerdes.OntomeClassLabelValue()));
        var geovClassLabelStream = builder
                .stream(input.TOPICS.geov_class_label,
                        Consumed.with(avroSerdes.GeovClassLabelKey(), avroSerdes.GeovClassLabelValue()));
        var projectClassStream = builder
                .stream(input.TOPICS.project_class,
                        Consumed.with(avroSerdes.ProjectClassKey(), avroSerdes.ProjectClassValue()));

        return addProcessors(
                builder,
                ontomeClassLabelStream,
                geovClassLabelStream,
                projectClassStream
        ).build();
    }

    public static final Integer DEFAULT_PROJECT = 375669;

    public static StreamsBuilder addProcessors(
            StreamsBuilder builder,
            KStream<OntomeClassLabelKey, OntomeClassLabelValue> ontomeClassLabelStream,
            KStream<GeovClassLabelKey, GeovClassLabelValue> geovClassLabelStream,
            KStream<ProjectClassKey, ProjectClassValue> projectClassStream
    ) {

        var avroSerdes = new ConfluentAvroSerdes();

        /* SOURCE PROCESSORS */

        // register project
        var projectTable = builder
                .table(input.TOPICS.project,
                        Consumed.with(avroSerdes.ProProjectKey(), avroSerdes.ProProjectValue()));
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
                projectTable,
                projectClassValue -> dev.projects.project.Key.newBuilder().setPkEntity(projectClassValue.getProjectId()).build(),
                (value1, value2) -> ProjectClassLanguageValue.newBuilder()
                        .setClassId(value1.getClassId())
                        .setProjectId(value1.getProjectId())
                        .setLanguageId(value2.getFkLanguage())
                        .setDeleted$1(Boolean.TRUE.equals(value1.getDeleted$1()) || Objects.equals(value2.getDeleted$1(), "true"))
                        .build(),
                Named.as("project_class_language"),
                Materialized.with(avroSerdes.ProjectClassKey(), avroSerdes.ProjectClassLanguageValue())
        );
        // 3
        var projectClassLabelOptionsTable = projectClassLanguage
                .toStream()
                .flatMap((key, value) -> {
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

      /*              // if project is not default project (DEFAULT_PROJECT) add default project record
                    if (value.getProjectId() != DEFAULT_PROJECT) {
                        // add record for default project in project's language
                        var kDefaultProjectProjectLang = ProjectClassLanguageKey.newBuilder()
                                .setClassId(value.getClassId())
                                .setProjectId(value.getProjectId())
                                .setLanguageId(value.getLanguageId())
                                .build();
                        var vDefaultProjectProjectLang = ProjectClassLanguageValue.newBuilder()
                                .setClassId(value.getClassId())
                                .setProjectId(DEFAULT_PROJECT)
                                .setLanguageId(value.getLanguageId())
                                .setDeleted$1(value.getDeleted$1())
                                .build();
                        result.add(KeyValue.pair(kDefaultProjectProjectLang, vDefaultProjectProjectLang));

                        // if language is not english (18889) add english record
                        if (value.getLanguageId() != 18889) {
                            // add record for default project english
                            var kDefaultProjectEnglish = ProjectClassLanguageKey.newBuilder()
                                    .setClassId(value.getClassId())
                                    .setProjectId(value.getProjectId())
                                    .setLanguageId(18889)
                                    .build();
                            var vDefaultProjectEnglish = ProjectClassLanguageValue.newBuilder()
                                    .setClassId(value.getClassId())
                                    .setProjectId(DEFAULT_PROJECT)
                                    .setLanguageId(18889)
                                    .setDeleted$1(value.getDeleted$1())
                                    .build();
                            result.add(KeyValue.pair(kDefaultProjectEnglish, vDefaultProjectEnglish));
                        }
                    }
*/
                    return result;
                })
                .toTable(
                        Named.as(inner.TOPICS.project_class_label_options),
                        Materialized.with(avroSerdes.ProjectClassLanguageKey(), avroSerdes.ProjectClassLanguageValue())
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
                Named.as("project_class_label_options_with_geov"),
                Materialized.with(avroSerdes.ProjectClassLanguageKey(), avroSerdes.ProjectClassLabelOptionMapValue())

        );
// 4b) left join geov default
        var withGeovDefault = withGeov.leftJoin(
                geovClassLabelTable,
                projectClassLanguageValue -> GeovClassLabelKey.newBuilder()
                        .setClassId(projectClassLanguageValue.getClassId())
                        .setProjectId(DEFAULT_PROJECT)
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
                Named.as("project_class_label_options_with_geov_and_default"),
                Materialized.with(avroSerdes.ProjectClassLanguageKey(), avroSerdes.ProjectClassLabelOptionMapValue())

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
                Named.as("project_class_label_options_with_geov_and_default_and_ontome"),
                Materialized.with(avroSerdes.ProjectClassLanguageKey(), avroSerdes.ProjectClassLabelOptionMapValue())
        );


// 6) group by
        var projectClassLabelOptionsGrouped = withGeovAndOntome.groupBy(
                (key, value) -> KeyValue.pair(
                        ProjectClassLabelKey.newBuilder()
                                .setProjectId(key.getProjectId())
                                .setClassId(key.getClassId())
                                .build(),
                        value),
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
                (aggKey, oldValue, aggValue) -> aggValue,
                Named.as(inner.TOPICS.project_class_label_options_aggregated),
                Materialized.<ProjectClassLabelKey, ProjectClassLabelOptionMap, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.project_class_label_options_aggregated)
                        .withKeySerde(avroSerdes.ProjectClassLabelKey())
                        .withValueSerde(avroSerdes.ProjectClassLabelOptionMapValue())
        );
        var projectClassLabel = projectClassLabelOptionsAggregated.mapValues((readOnlyKey, map) -> {
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
        }).toStream();

        /* SINK PROCESSORS */

        // 8) to
        projectClassLabel.to(output.TOPICS.project_class_label,
                Produced.with(avroSerdes.ProjectClassLabelKey(), avroSerdes.ProjectClassLabelValue()));

        return builder;

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

    public enum input {
        TOPICS;
        public final String ontome_class_label = OntomeClassLabel.output.TOPICS.ontome_class_label;
        public final String geov_class_label = GeovClassLabel.output.TOPICS.geov_class_label;
        public final String project_class = ProjectClass.output.TOPICS.project_class;

        public final String project = Utils.dbPrefixed("projects.project");
    }


    public enum inner {
        TOPICS;
        public final String project_class_label_options = "project_class_label_options";
        public final String project_class_label_options_grouped = "project_class_label_options_grouped";
        public final String project_class_label_options_aggregated = "project_class_label_options_aggregated";

    }

    public enum output {
        TOPICS;
        public final String project_class_label = Utils.tsPrefixed("project_class_label");
    }

}
