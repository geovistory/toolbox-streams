package org.geovistory.toolbox.streams.topologies;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.geovistory.toolbox.streams.app.DbTopicNames;
import org.geovistory.toolbox.streams.app.I;
import org.geovistory.toolbox.streams.app.RegisterInputTopic;
import org.geovistory.toolbox.streams.avro.CommunityEntityLabelConfigKey;
import org.geovistory.toolbox.streams.avro.CommunityEntityLabelConfigValue;
import org.geovistory.toolbox.streams.avro.EntityLabelConfig;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.Utils;

import java.util.LinkedList;
import java.util.List;


public class CommunityEntityLabelConfig {

    public static void main(String[] args) {
        System.out.println(buildStandalone(new StreamsBuilder()).describe());
    }

    public static Topology buildStandalone(StreamsBuilder builder) {
        var registerInputTopic = new RegisterInputTopic(builder);

        return addProcessors(
                builder,
                registerInputTopic.proEntityLabelConfigStream()
        ).builder().build();
    }


    public static CommunityEntityLabelConfigReturnValue addProcessors(
            StreamsBuilder builder,
            KStream<dev.projects.entity_label_config.Key, dev.projects.entity_label_config.Value> proEntityLabelConfigStream
    ) {
        var mapper = new ObjectMapper(); // create once, reuse
        var avroSerdes = new ConfluentAvroSerdes();
        /* STREAM PROCESSORS */
        // 2)
        var communityEntityLabelConfigStream = proEntityLabelConfigStream.flatMap(
                (key, value) -> {
                    List<KeyValue<CommunityEntityLabelConfigKey, CommunityEntityLabelConfigValue>> result = new LinkedList<>();
                    if (value.getFkProject() != I.DEFAULT_PROJECT.get()) return result;
                    try {
                        var config = mapper.readValue(value.getConfig(), EntityLabelConfig.class);
                        var kv = KeyValue.pair(
                                CommunityEntityLabelConfigKey.newBuilder()
                                        .setClassId(value.getFkClass())
                                        .build(),
                                CommunityEntityLabelConfigValue.newBuilder()
                                        .setClassId(value.getFkClass())
                                        .setConfig(config)
                                        .setDeleted$1(Utils.stringIsEqualTrue(value.getDeleted$1()))
                                        .build()
                        );
                        result.add(kv);
                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                        return null;
                    }
                    return result;
                }
        );
        /* SINK PROCESSORS */

        // 8) to
        communityEntityLabelConfigStream.to(output.TOPICS.community_entity_label_config,
                Produced.with(avroSerdes.CommunityEntityLabelConfigKey(), avroSerdes.CommunityEntityLabelConfigValue()));

        return new CommunityEntityLabelConfigReturnValue(builder, communityEntityLabelConfigStream);

    }
    public enum input {
        TOPICS;
        public final String entity_label_config = DbTopicNames.pro_entity_label_config.getName();
    }

    public enum output {
        TOPICS;
        public final String community_entity_label_config = Utils.tsPrefixed("community_entity_label_config");
    }

}
