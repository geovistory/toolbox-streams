package org.geovistory.toolbox.streams.project.config;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.InputTopicHelper;

/**
 * This class provides helper methods to register
 * source topics (topics consumed but not generated by this app)
 */
public class RegisterInputTopic extends InputTopicHelper {
    public ConfluentAvroSerdes avroSerdes;

    public RegisterInputTopic(StreamsBuilder builder) {
        super(builder);
        this.avroSerdes = new ConfluentAvroSerdes();
    }

    public KTable<dev.projects.project.Key, dev.projects.project.Value> proProjectTable() {
        return getRepartitionedTable(
                DbTopicNames.pro_projects.getName(),
                avroSerdes.ProProjectKey(),
                avroSerdes.ProProjectValue()
        );
    }

    public KStream<dev.projects.text_property.Key, dev.projects.text_property.Value> proTextPropertyStream() {
        return getRepartitionedStream(
                DbTopicNames.pro_text_property.getName(),
                avroSerdes.ProTextPropertyKey(),
                avroSerdes.ProTextPropertyValue()
        );
    }

    public KTable<dev.projects.dfh_profile_proj_rel.Key, dev.projects.dfh_profile_proj_rel.Value> proProfileProjRelTable() {
        return getRepartitionedTable(
                DbTopicNames.pro_dfh_profile_proj_rel.getName(),
                avroSerdes.ProProfileProjRelKey(),
                avroSerdes.ProProfileProjRelValue()
        );
    }

    public KTable<dev.system.config.Key, dev.system.config.Value> sysConfigTable() {
        return getRepartitionedTable(
                DbTopicNames.sys_config.getName(),
                avroSerdes.SysConfigKey(),
                avroSerdes.SysConfigValue()
        );
    }

    public KStream<dev.projects.entity_label_config.Key, dev.projects.entity_label_config.Value> proEntityLabelConfigStream() {
        return getStream(
                DbTopicNames.pro_entity_label_config.getName(),
                avroSerdes.ProEntityLabelConfigKey(),
                avroSerdes.ProEntityLabelConfigValue()
        );
    }

    public KStream<OntomeClassKey, OntomeClassValue> ontomeClassStream() {
        return builder.stream(Env.INSTANCE.TOPIC_ONTOME_CLASS,
                Consumed.with(avroSerdes.OntomeClassKey(), avroSerdes.OntomeClassValue()));
    }

    public KStream<OntomePropertyKey, OntomePropertyValue> ontomePropertyStream() {
        return builder.stream(Env.INSTANCE.TOPIC_ONTOME_PROPERTY,
                Consumed.with(avroSerdes.OntomePropertyKey(), avroSerdes.OntomePropertyValue()));
    }
    public KStream<OntomeClassLabelKey, OntomeClassLabelValue> ontomeClassLabelStream() {
        return builder.stream(Env.INSTANCE.TOPIC_ONTOME_CLASS_LABEL,
                Consumed.with(avroSerdes.OntomeClassLabelKey(), avroSerdes.OntomeClassLabelValue()));
    }

    public KStream<OntomePropertyLabelKey, OntomePropertyLabelValue> ontomePropertyLabelStream() {
        return builder.stream(Env.INSTANCE.TOPIC_ONTOME_PROPERTY_LABEL,
                Consumed.with(avroSerdes.OntomePropertyLabelKey(), avroSerdes.OntomePropertyLabelValue()));
    }

}