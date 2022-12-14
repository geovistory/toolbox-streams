package org.geovistory.toolbox.streams.topologies;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.app.DbTopicNames;
import org.geovistory.toolbox.streams.app.RegisterInputTopic;
import org.geovistory.toolbox.streams.avro.BooleanMap;
import org.geovistory.toolbox.streams.avro.ProjectProfileKey;
import org.geovistory.toolbox.streams.avro.ProjectProfileValue;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.ListSerdes;
import org.geovistory.toolbox.streams.lib.Utils;
import org.geovistory.toolbox.streams.lib.jsonmodels.SysConfigValue;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

public class ProjectProfiles {

    public static void main(String[] args) {
        System.out.println(buildStandalone(new StreamsBuilder()).describe());
    }

    public static Topology buildStandalone(StreamsBuilder builder) {
        var registerInputTopic = new RegisterInputTopic(builder);
        var proProjectTable = registerInputTopic.proProjectTable();
        var proProfileProjRelTable = registerInputTopic.proProfileProjRelTable();
       var sysConfigTable = registerInputTopic.sysConfigTable();
        return addProcessors(
                builder,
                proProjectTable,
                proProfileProjRelTable,
                sysConfigTable
        ).builder().build();
    }

    public static ProjectProfilesReturnValue addProcessors(
            StreamsBuilder builder,
            KTable<dev.projects.project.Key, dev.projects.project.Value> proProjects,
            KTable<dev.projects.dfh_profile_proj_rel.Key, dev.projects.dfh_profile_proj_rel.Value> proDfhProfileProjRels,
            KTable<dev.system.config.Key, dev.system.config.Value> sysConfig
    ) {
        ObjectMapper mapper = new ObjectMapper(); // create once, reuse
        String SYS_CONFIG = "SYS_CONFIG";
        String REQUIRED_ONTOME_PROFILES = "REQUIRED_ONTOME_PROFILES";
        var avroSerdes = new ConfluentAvroSerdes();
        var listSerdes = new ListSerdes();


        /* STREAM PROCESSORS */


        // 2)
        var profilesByProject = proDfhProfileProjRels
                // Filter: only enabled profiles project relations
                .filter((k, v) -> v.getEnabled() && !Objects.equals(v.getDeleted$1(), "true"))
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
                Named.as(inner.TOPICS.projects_with_enabled_profiles),
                Materialized.with(Serdes.Integer(), listSerdes.IntegerList())
        );

        // 5)
        // Key: constant "REQUIRED_ONTOME_PROFILES", Value: List of profile ids
        KTable<String, List<Integer>> requiredProfiles = sysConfig
                .toStream()
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
                        Named.as(inner.TOPICS.required_profiles),
                        Materialized
                                .with(Serdes.String(), listSerdes.IntegerList()));
        // 7)
        // Key: project, Val: project
        KTable<Integer, Integer> projectKeys = proProjects
                .toStream()
                .map((k, v) -> new KeyValue<>(k.getPkEntity(), v == null ? null : k.getPkEntity()))
                // 8)
                .toTable(
                        Named.as(inner.TOPICS.project_keys),
                        Materialized
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
                Materialized
                        .<Integer, List<Integer>, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.projects_with_required_profiles)
                        .withKeySerde(Serdes.Integer())
                        .withValueSerde(listSerdes.IntegerList())
        );

        // 10)
        // Key: project, Val: profiles (required + enabled)
        KTable<Integer, List<Integer>> projectWithProfiles = projectsWithRequiredProfiles.leftJoin(
                projectsWithEnabledProfiles,
                (value1, value2) -> {
                    if (value2 != null) value1.addAll(value2);
                    return value1;
                },
                Named.as(inner.TOPICS.projects_with_profiles),
                Materialized.with(Serdes.Integer(), listSerdes.IntegerList())
        );
        // 11
        KStream<ProjectProfileKey, ProjectProfileValue> projectProfileStream;
        projectProfileStream = projectWithProfiles.toStream().mapValues(
                        (readOnlyKey, value) -> {
                            var map = BooleanMap.newBuilder().build();
                            var __deleted = false;
                            if (value != null) value.forEach(profileId -> map.getItem().put(profileId.toString(), __deleted));
                            return map;
                        }
                )
                // 12
                .groupByKey(
                        Grouped.with(Serdes.Integer(), avroSerdes.BooleanMapValue())
                )
                //13
                .reduce((aggValue, newValue) -> {
                    aggValue.getItem().forEach((profileId, __deleted) -> {
                        // compare oldValue with newValue and mark profiles of oldValue
                        // as deleted, if they are absent in newValue
                        if (!__deleted) newValue.getItem().putIfAbsent(profileId, true);
                    });
                    return newValue;
                })
                .toStream()
                // 14
                .flatMap(
                        (projectId, profilesMap) -> {
                            List<KeyValue<ProjectProfileKey, ProjectProfileValue>> result = new LinkedList<>();
                            profilesMap.getItem().forEach((profileId, deleted) -> {
                                var k = ProjectProfileKey.newBuilder()
                                        .setProjectId(projectId).setProfileId(Integer.parseInt(profileId)).build();
                                var v = ProjectProfileValue.newBuilder()
                                        .setProjectId(projectId).setProfileId(Integer.parseInt(profileId))
                                        .setDeleted$1(deleted).build();
                                result.add(KeyValue.pair(k, v));
                            });
                            return result;
                        }
                );

        /* SINK PROCESSOR */
        projectProfileStream.to(output.TOPICS.project_profile,
                Produced.with(avroSerdes.ProjectProfileKey(), avroSerdes.ProjectProfileValue()));

        return new ProjectProfilesReturnValue(builder, projectProfileStream);

    }

    public enum input {
        TOPICS;
        public final String dfh_profile_proj_rel = DbTopicNames.pro_dfh_profile_proj_rel.getName();
        public final String project = DbTopicNames.pro_projects.getName();
        public final String config = DbTopicNames.sys_config.getName();
    }


    public enum inner {
        TOPICS;
        public final String profiles_grouped_by_projects = Utils.tsPrefixed("profiles_grouped_by_projects");
        public final String projects_with_enabled_profiles = Utils.tsPrefixed("projects_with_enabled_profiles");
        public final String projects_with_required_profiles = Utils.tsPrefixed("projects_with_required_profiles");
        public final String projects_with_profiles = Utils.tsPrefixed("projects_with_profiles");
        public final String required_profiles = Utils.tsPrefixed("required_profiles");
        public final String project_keys = Utils.tsPrefixed("project_keys");

    }

    public enum output {
        TOPICS;
        public final String project_profile = Utils.tsPrefixed("project_profile");
    }


}
