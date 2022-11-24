package org.geovistory.toolbox.streams.app;

import com.fasterxml.jackson.annotation.ObjectIdGenerator;
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
import java.util.LinkedList;
import java.util.List;


public class ProjectPropertyTopology {

    public static void main(String[] args) {
        System.out.println(build(new StreamsBuilder()).describe());
    }

    public static Topology build(StreamsBuilder builder) {
        return addProcessors(builder).build();
    }

    public static StreamsBuilder addProcessors(StreamsBuilder builder) {


        /* SOURCE PROCESSORS */

        // 1)
        // register projects_with_aggregated_profiles
        KStream<Id, ProfileIds> projectWithAggProfiles = builder
                .stream(input.TOPICS.projects_with_aggregated_profiles,
                        Consumed.with(AvroSerdes.IdKey(), AvroSerdes.ProfileIdsValue()));

        // register api_property
        KTable<dev.data_for_history.api_property.Key, dev.data_for_history.api_property.Value> apiProperty = builder
                .table(input.TOPICS.api_property,
                        Consumed.with(AvroSerdes.DfhApiPropertyKey(), AvroSerdes.DfhApiPropertyValue()));



        /* STREAM PROCESSORS */


// 2)

        var profileProject = projectWithAggProfiles
                .flatMap(
                        (projectId, value) -> {
                            List<KeyValue<Integer, Integer>> projectProfile = new LinkedList<>();
                            value.getProfileIds().forEach(profileId -> projectProfile.add(KeyValue.pair(profileId, projectId.getId())));
                            return projectProfile;
                        }
                );
// 3)



        return builder;

    }


    public enum input {
        TOPICS;
        public final String projects_with_aggregated_profiles = ProjectProfilesTopology.output.TOPICS.projects_with_aggregated_profiles;
        public final String api_property = Utils.prefixed("data_for_history.api_property");
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
