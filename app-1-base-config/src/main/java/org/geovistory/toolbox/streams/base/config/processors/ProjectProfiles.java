package org.geovistory.toolbox.streams.base.config.processors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.BooleanMap;
import org.geovistory.toolbox.streams.avro.IntegerList;
import org.geovistory.toolbox.streams.avro.ProjectProfileKey;
import org.geovistory.toolbox.streams.avro.ProjectProfileValue;
import org.geovistory.toolbox.streams.base.config.OutputTopicNames;
import org.geovistory.toolbox.streams.base.config.RegisterInnerTopic;
import org.geovistory.toolbox.streams.base.config.RegisterInputTopic;
import org.geovistory.toolbox.streams.lib.ConfiguredAvroSerde;
import org.geovistory.toolbox.streams.lib.IdenticalRecordsFilterSupplierMemory;
import org.geovistory.toolbox.streams.lib.Utils;
import org.geovistory.toolbox.streams.lib.jsonmodels.SysConfigValue;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

@ApplicationScoped
public class ProjectProfiles {
    @Inject
    ConfiguredAvroSerde as;

    @Inject
    RegisterInputTopic registerInputTopic;

    @Inject
    RegisterInnerTopic registerInnerTopic;

    @Inject
    OutputTopicNames outputTopicNames;

    public ProjectProfiles(ConfiguredAvroSerde as, RegisterInputTopic registerInputTopic, RegisterInnerTopic registerInnerTopic, OutputTopicNames outputTopicNames) {
        this.as = as;
        this.registerInputTopic = registerInputTopic;
        this.registerInnerTopic = registerInnerTopic;
        this.outputTopicNames = outputTopicNames;
    }

    public void addProcessorsStandalone() {
        var proProjectTable = registerInputTopic.proProjectTable();
        var proProfileProjRelTable = registerInputTopic.proProfileProjRelTable();
        var sysConfigStream = registerInputTopic.sysConfigStream();
        addProcessors(
                proProjectTable,
                proProfileProjRelTable,
                sysConfigStream
        );
    }

    public ProjectProfilesReturnValue addProcessors(
            KTable<dev.projects.project.Key, dev.projects.project.Value> proProjects,
            KTable<dev.projects.dfh_profile_proj_rel.Key, dev.projects.dfh_profile_proj_rel.Value> proDfhProfileProjRels,
            KStream<dev.system.config.Key, dev.system.config.Value> sysConfig
    ) {
        ObjectMapper mapper = new ObjectMapper(); // create once, reuse
        String SYS_CONFIG = "SYS_CONFIG";
        String REQUIRED_ONTOME_PROFILES = "REQUIRED_ONTOME_PROFILES";


        /* STREAM PROCESSORS */


        // 2)
        var profilesByProject = proDfhProfileProjRels
                // Filter: only enabled profiles project relations
                .filter(
                        (k, v) -> v.getEnabled() && !Objects.equals(v.getDeleted$1(), "true"),
                        Named.as("ktable-filter-enabled-project-profile-relations")
                )
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
        KTable<Integer, IntegerList> projectsWithEnabledProfiles = profilesByProject.aggregate(
                // initializer
                () -> IntegerList.newBuilder().build(),

                // adder
                (k, v, agg) -> {
                    agg.getList().add(v);
                    return agg;
                },
                // subtractor
                (k, v, agg) -> {
                    var i = agg.getList().indexOf(v);
                    if (i > -1) agg.getList().remove(i);
                    return agg;
                },
                Named.as(inner.TOPICS.projects_with_enabled_profiles),
                Materialized.
                        <Integer, IntegerList, KeyValueStore<Bytes, byte[]>>as("inner.TOPICS.projects_with_enabled_profiles")
                        .withKeySerde(Serdes.Integer())
                        .withValueSerde(as.value())
        );

        // 5)
        // Key: constant "REQUIRED_ONTOME_PROFILES", Value: List of profile ids
        KTable<String, IntegerList> requiredProfiles = sysConfig
                .filter(
                        ((k, v) -> v.getKey() != null && v.getKey().equals(SYS_CONFIG)),
                        Named.as("kstream-filter-sys-config")
                )
                // 6)
                .map((k, v) -> {
                            IntegerList p = IntegerList.newBuilder().build();
                            try {
                                var profiles = mapper.readValue(v.getConfig(), SysConfigValue.class).ontome.requiredOntomeProfiles;
                                p.setList(profiles);
                            } catch (JsonProcessingException e) {
                                e.printStackTrace();
                            }
                            return new KeyValue<>(REQUIRED_ONTOME_PROFILES, p);
                        },
                        Named.as("kstream-map-required-ontome-profiles")
                )
                .repartition(
                        Repartitioned.<String, IntegerList>as(inner.TOPICS.required_profiles + "-repartition")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(as.value())
                                .withName(inner.TOPICS.required_profiles + "-repartition-2")
                )
                .transform(
                        new IdenticalRecordsFilterSupplierMemory<>(inner.TOPICS.required_profiles + "-deduplicate",
                                Serdes.String(),
                                as.value())
                )
                .toTable(
                        Named.as(inner.TOPICS.required_profiles),
                        Materialized
                                .<String, IntegerList, KeyValueStore<Bytes, byte[]>>
                                        as(inner.TOPICS.required_profiles + "-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(as.value())
                );

        // 7)
        // Key: project, Val: project
        KTable<Integer, Integer> projectKeys = proProjects
                .toStream(
                        Named.as(inner.TOPICS.required_profiles + "-to-stream")
                )
                .map(
                        (k, v) -> new KeyValue<>(k.getPkEntity(), v == null ? null : k.getPkEntity()),
                        Named.as("kstream-map-project-ids")
                )
                // 8)
                .toTable(
                        Named.as(inner.TOPICS.project_keys),
                        Materialized
                                .with(Serdes.Integer(), Serdes.Integer()));
        // 9)
        // Key: project, Val: required profiles
        KTable<Integer, IntegerList> projectsWithRequiredProfiles = projectKeys.leftJoin(
                requiredProfiles,
                (v) -> REQUIRED_ONTOME_PROFILES,
                (leftVal, rightVal) ->
                        leftVal == null ?
                                null : rightVal == null ?
                                IntegerList.newBuilder().build() : rightVal,
                TableJoined.as(inner.TOPICS.projects_with_required_profiles + "-fk-left-join"),
                Materialized
                        .<Integer, IntegerList, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.projects_with_required_profiles)
                        .withKeySerde(Serdes.Integer())
                        .withValueSerde(as.value())
        );

        // 10)
        // Key: project, Val: profiles (required + enabled)
        KTable<Integer, IntegerList> projectWithProfiles = projectsWithRequiredProfiles.leftJoin(
                projectsWithEnabledProfiles,
                (value1, value2) -> {
                    if (value2 != null && value2.getList().size() > 0) value1.getList().addAll(value2.getList());
                    return value1;
                },
                Named.as(inner.TOPICS.projects_with_profiles + "-fk-left-join"),
                Materialized
                        .<Integer, IntegerList, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.projects_with_profiles)
                        .withKeySerde(Serdes.Integer())
                        .withValueSerde(as.value())
        );
        // 11
        KStream<ProjectProfileKey, ProjectProfileValue> projectProfileStream;
        projectProfileStream = projectWithProfiles.toStream(
                        Named.as(inner.TOPICS.projects_with_profiles + "-to-stream")
                ).mapValues(
                        (readOnlyKey, value) -> {
                            var map = BooleanMap.newBuilder().build();
                            var __deleted = false;
                            if (value != null)
                                value.getList().forEach(profileId -> map.getItem().put(profileId.toString(), __deleted));
                            return map;
                        },
                        Named.as("kstream-mapvalues-integer-list-to-boolean-map")
                )
                // 12
                .groupByKey(
                        Grouped.with(Serdes.Integer(), as.<BooleanMap>value())
                                .withName("kstream-group-by-key-project-profile-map")
                )
                //13
                .reduce(
                        (aggValue, newValue) -> {
                            aggValue.getItem().forEach((profileId, __deleted) -> {
                                // compare oldValue with newValue and mark profiles of oldValue
                                // as deleted, if they are absent in newValue
                                if (!__deleted) newValue.getItem().putIfAbsent(profileId, true);
                            });
                            return newValue;
                        }
                )
                .toStream(
                        Named.as(inner.TOPICS.projects_with_profiles + "_aggregated" + "-to-stream")
                )
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
                        },
                        Named.as("kstream-flatmap-project-profiles-map-to-project-profile")
                );

        /* SINK PROCESSOR */
        projectProfileStream.to(outputTopicNames.projectProfile(),
                Produced.with(as.<ProjectProfileKey>key(), as.<ProjectProfileValue>value())
                        .withName(outputTopicNames.projectProfile() + "-producer")
        );

        return new ProjectProfilesReturnValue(projectProfileStream);

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


}
