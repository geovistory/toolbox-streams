package org.geovistory.toolbox.streams.lib;

import io.apicurio.registry.serde.avro.AvroKafkaDeserializer;
import io.apicurio.registry.serde.avro.AvroKafkaSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.geovistory.toolbox.streams.avro.*;


public class AvroSerdes {

    public static Serde<dev.projects.dfh_profile_proj_rel.Key> ProProfileProjRelKey() {
        AvroKafkaSerializer<dev.projects.dfh_profile_proj_rel.Key> serializer = new AvroKafkaSerializer<>();
        AvroKafkaDeserializer<dev.projects.dfh_profile_proj_rel.Key> deserializer = new AvroKafkaDeserializer<>();
        Serde<dev.projects.dfh_profile_proj_rel.Key> serdes = Serdes.serdeFrom(serializer, deserializer);
        serdes.configure(AvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }

    public static Serde<dev.projects.dfh_profile_proj_rel.Value> ProProfileProjRelValue() {
        AvroKafkaSerializer<dev.projects.dfh_profile_proj_rel.Value> serializer = new AvroKafkaSerializer<>();
        AvroKafkaDeserializer<dev.projects.dfh_profile_proj_rel.Value> deserializer = new AvroKafkaDeserializer<>();
        Serde<dev.projects.dfh_profile_proj_rel.Value> serdes = Serdes.serdeFrom(serializer, deserializer);
        serdes.configure(AvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public static Serde<dev.projects.project.Key> ProProjectKey() {
        AvroKafkaSerializer<dev.projects.project.Key> serializer = new AvroKafkaSerializer<>();
        AvroKafkaDeserializer<dev.projects.project.Key> deserializer = new AvroKafkaDeserializer<>();
        Serde<dev.projects.project.Key> serdes = Serdes.serdeFrom(serializer, deserializer);
        serdes.configure(AvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }

    public static Serde<dev.projects.project.Value> ProProjectValue() {
        AvroKafkaSerializer<dev.projects.project.Value> serializer = new AvroKafkaSerializer<>();
        AvroKafkaDeserializer<dev.projects.project.Value> deserializer = new AvroKafkaDeserializer<>();
        Serde<dev.projects.project.Value> serdes = Serdes.serdeFrom(serializer, deserializer);
        serdes.configure(AvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public static Serde<dev.system.config.Key> SysConfigKey() {
        AvroKafkaSerializer<dev.system.config.Key> serializer = new AvroKafkaSerializer<>();
        AvroKafkaDeserializer<dev.system.config.Key> deserializer = new AvroKafkaDeserializer<>();
        Serde<dev.system.config.Key> serdes = Serdes.serdeFrom(serializer, deserializer);
        serdes.configure(AvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }

    public static Serde<dev.system.config.Value> SysConfigValue() {
        AvroKafkaSerializer<dev.system.config.Value> serializer = new AvroKafkaSerializer<>();
        AvroKafkaDeserializer<dev.system.config.Value> deserializer = new AvroKafkaDeserializer<>();
        Serde<dev.system.config.Value> serdes = Serdes.serdeFrom(serializer, deserializer);
        serdes.configure(AvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public static Serde<Id> IdKey() {
        AvroKafkaSerializer<Id> serializer = new AvroKafkaSerializer<>();
        AvroKafkaDeserializer<Id> deserializer = new AvroKafkaDeserializer<>();
        Serde<Id> serdes = Serdes.serdeFrom(serializer, deserializer);
        serdes.configure(AvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }


    public static Serde<ProjectProfileId> ProjectProfileIdKey() {
        AvroKafkaSerializer<ProjectProfileId> serializer = new AvroKafkaSerializer<>();
        AvroKafkaDeserializer<ProjectProfileId> deserializer = new AvroKafkaDeserializer<>();
        Serde<ProjectProfileId> serdes = Serdes.serdeFrom(serializer, deserializer);
        serdes.configure(AvroSerdesConfig.INSTANCE.getConfig(), true);

        return serdes;
    }

    public static Serde<Dummy> DummyValue() {
        AvroKafkaSerializer<Dummy> serializer = new AvroKafkaSerializer<>();
        AvroKafkaDeserializer<Dummy> deserializer = new AvroKafkaDeserializer<>();
        Serde<Dummy> serdes = Serdes.serdeFrom(serializer, deserializer);
        serdes.configure(AvroSerdesConfig.INSTANCE.getConfig(), false);

        return serdes;
    }


    public static Serde<BooleanMap> BooleanMapValue() {
        AvroKafkaSerializer<BooleanMap> serializer = new AvroKafkaSerializer<>();
        AvroKafkaDeserializer<BooleanMap> deserializer = new AvroKafkaDeserializer<>();
        Serde<BooleanMap> serdes = Serdes.serdeFrom(serializer, deserializer);
        serdes.configure(AvroSerdesConfig.INSTANCE.getConfig(), false);

        return serdes;
    }

    public static Serde<ProfileIds> ProfileIdsValue() {
        AvroKafkaSerializer<ProfileIds> serializer = new AvroKafkaSerializer<>();
        AvroKafkaDeserializer<ProfileIds> deserializer = new AvroKafkaDeserializer<>();
        Serde<ProfileIds> serdes = Serdes.serdeFrom(serializer, deserializer);
        serdes.configure(AvroSerdesConfig.INSTANCE.getConfig(), false);

        return serdes;
    }

}
