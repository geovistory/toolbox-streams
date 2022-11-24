package org.geovistory.toolbox.streams.app;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.geovistory.toolbox.streams.avro.Id;
import org.geovistory.toolbox.streams.avro.ProfileIds;
import org.geovistory.toolbox.streams.lib.AvroSerdes;
import org.geovistory.toolbox.streams.lib.ListSerdes;
import org.geovistory.toolbox.streams.lib.Utils;
import org.geovistory.toolbox.streams.lib.jsonmodels.SysConfigValue;

import java.util.ArrayList;
import java.util.List;


public class ProjectProfilesTopology {

    public static void main(String[] args) {
        System.out.println(build(new StreamsBuilder()).describe());
    }

    public static Topology build(StreamsBuilder builder) {
        return addProcessors(builder).build();
    }

    public static StreamsBuilder addProcessors(StreamsBuilder builder) {
        ObjectMapper mapper = new ObjectMapper(); // create once, reuse
        String SYS_CONFIG = "SYS_CONFIG";
        String REQUIRED_ONTOME_PROFILES = "REQUIRED_ONTOME_PROFILES";

        /* SOURCE PROCESSORS */

        // 1)
        // register projects
        KStream<dev.projects.project.Key, dev.projects.project.Value> projects = builder
                .stream(input.TOPICS.project,
                        Consumed.with(AvroSerdes.ProProjectKey(), AvroSerdes.ProProjectValue()));

        // register dfh_profile_proj_rel
        KTable<dev.projects.dfh_profile_proj_rel.Key, dev.projects.dfh_profile_proj_rel.Value> dfhProfileProjRels = builder
                .table(input.TOPICS.dfh_profile_proj_rel,
                        Consumed.with(AvroSerdes.ProProfileProjRelKey(), AvroSerdes.ProProfileProjRelValue()));

        // register config
        KStream<dev.system.config.Key, dev.system.config.Value> config = builder
                .stream(input.TOPICS.config,
                        Consumed.with(AvroSerdes.SysConfigKey(), AvroSerdes.SysConfigValue()));

        /* STREAM PROCESSORS */


// 2)
        var profilesByProject = dfhProfileProjRels
                // Filter: only enabled profiles project relations
                .filter((k, v) -> v.getEnabled())
// 3)
                // Group: by project
                .groupBy(
                        (k, v) -> new KeyValue<>(v.getFkProject(), v.getFkProfile()),
                        Grouped.with(
                                inner.TOPICS.profiles_grouped_by_projects,
                                Serdes.Integer(),
                                Serdes.Integer()
                        )
                );
// 4)
        // Aggregate: key: project, value: array of profiles
        KTable<Integer, List<Integer>> projectsWithEnabledProfiles = profilesByProject.aggregate(
                // initializer
                ArrayList::new,
                // adder
                (k, v, agg) -> {
                    agg.add(v);
                    return agg;
                },
                // subtractor
                (k, v, agg) -> {
                    agg.remove(v);
                    return agg;
                },
                Named.as(inner.TOPICS.projects_with_enabled_profiles_store),
                Materialized.with(Serdes.Integer(), ListSerdes.IntegerList())
        );

// 5)
        // Key: constant "REQUIRED_ONTOME_PROFILES", Value: List of profile ids
        KTable<String, List<Integer>> requiredProfiles = config
                .filter(((k, v) -> v.getKey().equals(SYS_CONFIG)))
// 6)
                .map((k, v) -> {
                    List<Integer> p = new ArrayList<>();
                    try {
                        p = mapper.readValue(v.getConfig(), SysConfigValue.class).ontome.requiredOntomeProfiles;
                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                    }
                    return new KeyValue<>(REQUIRED_ONTOME_PROFILES, p);
                })
                .toTable(
                        Materialized
                                .with(Serdes.String(), ListSerdes.IntegerList()));
// 7)
        // Key: project, Val: project
        KTable<Integer, Integer> projectKeys = projects.map((k, v) -> new KeyValue<>(k.getPkEntity(), v == null ? null : k.getPkEntity()))
// 8)
                .toTable(Materialized
                        .with(Serdes.Integer(), Serdes.Integer()));
// 9)
        // Key: project, Val: required profiles
        KTable<Integer, List<Integer>> projectsWithRequiredProfiles = projectKeys.leftJoin(
                requiredProfiles,
                (v) -> REQUIRED_ONTOME_PROFILES,
                (leftVal, rightVal) ->
                        leftVal == null ?
                                null : rightVal == null ?
                                new ArrayList<>() : rightVal,
                Materialized.with(Serdes.Integer(), ListSerdes.IntegerList())
        );

// 10)
        // Key: project, Val: profiles (required + enabled)
        KTable<Integer, List<Integer>> finalTable = projectsWithRequiredProfiles.leftJoin(
                projectsWithEnabledProfiles,
                (value1, value2) -> {
                    if (value2 != null) value1.addAll(value2);
                    return value1;
                },
                Materialized.with(Serdes.Integer(), ListSerdes.IntegerList())
        );



        /* SINK PROCESSOR */
        finalTable.toStream()
                .map((key, value) -> new KeyValue<>(new Id(key), value == null ? null : new ProfileIds(value)))
                .to(output.TOPICS.projects_with_aggregated_profiles, Produced.with(AvroSerdes.IdKey(), AvroSerdes.ProfileIdsValue()));

        return builder;

    }


    public enum input {
        TOPICS;
        public final String dfh_profile_proj_rel = Utils.prefixed("projects.dfh_profile_proj_rel");
        public final String project = Utils.prefixed("projects.project");
        public final String config = Utils.prefixed("system.config");
    }

    public enum inner {
        TOPICS;
        public final String profiles_grouped_by_projects = Utils.prefixed("profiles_grouped_by_projects");
        public final String projects_with_enabled_profiles_store = Utils.prefixed("projects_with_aggregated_profiles_store");
    }


    public enum output {
        TOPICS;
        public final String projects_with_aggregated_profiles = Utils.prefixed("projects_with_aggregated_profiles");
    }

}
