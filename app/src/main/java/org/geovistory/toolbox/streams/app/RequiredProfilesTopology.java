package org.geovistory.toolbox.streams.app;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.BooleanMap;
import org.geovistory.toolbox.streams.lib.AvroSerdes;
import org.geovistory.toolbox.streams.lib.Utils;
import org.geovistory.toolbox.streams.lib.jsonmodels.SysConfigValue;

import java.util.LinkedList;
import java.util.List;


public class RequiredProfilesTopology {

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

        // register config
        KStream<dev.system.config.Key, dev.system.config.Value> config = builder
                .stream(input.TOPICS.config,
                        Consumed.with(AvroSerdes.SysConfigKey(), AvroSerdes.SysConfigValue()));

        /* STREAM PROCESSORS */

        config
                .filter(((k, v) -> v.getKey().equals(SYS_CONFIG)))
                .map((k, v) -> {
                    var booleanMap = BooleanMap.newBuilder().build();
                    var map = booleanMap.getMap();
                    try {
                        mapper.readValue(v.getConfig(), SysConfigValue.class).ontome.requiredOntomeProfiles
                                .forEach(integer -> map.put(integer.toString(), true));
                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                    }
                    return new KeyValue<>(REQUIRED_ONTOME_PROFILES, booleanMap);
                })
                .groupByKey(
                        Grouped.as(inner.TOPICS.sysconfig_grouped)
                                .with(Serdes.String(), AvroSerdes.BooleanMapValue()))
                .reduce(
                        // adder
                        (aggValue, newValue) -> {
                            // for each old value not present in aggValue add tombstone
                            aggValue.getMap().forEach((key, aBoolean) -> {
                                var map = newValue.getMap();
                                if (aBoolean) map.putIfAbsent(key, false);
                            });
                            return newValue;
                        }
                )
                .toStream()
                .flatMap((k, v) -> {
                    List<KeyValue<Integer, Integer>> result = new LinkedList<>();
                    v.getMap().forEach((key, aBoolean) -> {
                        Integer profileId = Integer.parseInt(key);
                        result.add(new KeyValue<>(profileId, aBoolean ? profileId : null));
                    });
                    return result;
                })
                /* SINK PROCESSOR */
                .to(output.TOPICS.required_profiles, Produced.with(Serdes.Integer(), Serdes.Integer()));

        return builder;

    }

    public enum input {
        TOPICS;
        public final String config = Utils.prefixed("system.config");
    }


    public enum inner {
        TOPICS;
        public final String sysconfig_grouped = Utils.prefixed("sysconfig_grouped");
    }

    public enum output {
        TOPICS;
        public final String required_profiles = Utils.prefixed("required_profiles");
    }
}
