package org.geovistory.toolbox.streams.lib;

import io.apicurio.registry.serde.avro.AvroKafkaDeserializer;
import io.apicurio.registry.serde.avro.AvroKafkaSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.geovistory.toolbox.streams.avro.*;


public class AvroSerdes {

    public Serde<dev.projects.dfh_profile_proj_rel.Key> ProProfileProjRelKey() {
        AvroKafkaSerializer<dev.projects.dfh_profile_proj_rel.Key> serializer = new AvroKafkaSerializer<>();
        AvroKafkaDeserializer<dev.projects.dfh_profile_proj_rel.Key> deserializer = new AvroKafkaDeserializer<>();
        Serde<dev.projects.dfh_profile_proj_rel.Key> serdes = Serdes.serdeFrom(serializer, deserializer);
        serdes.configure(AvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }

    public Serde<dev.projects.dfh_profile_proj_rel.Value> ProProfileProjRelValue() {
        AvroKafkaSerializer<dev.projects.dfh_profile_proj_rel.Value> serializer = new AvroKafkaSerializer<>();
        AvroKafkaDeserializer<dev.projects.dfh_profile_proj_rel.Value> deserializer = new AvroKafkaDeserializer<>();
        Serde<dev.projects.dfh_profile_proj_rel.Value> serdes = Serdes.serdeFrom(serializer, deserializer);
        serdes.configure(AvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }


    public Serde<dev.data_for_history.api_property.Key> DfhApiPropertyKey() {
        AvroKafkaSerializer<dev.data_for_history.api_property.Key> serializer = new AvroKafkaSerializer<>();
        AvroKafkaDeserializer<dev.data_for_history.api_property.Key> deserializer = new AvroKafkaDeserializer<>();
        Serde<dev.data_for_history.api_property.Key> serdes = Serdes.serdeFrom(serializer, deserializer);
        serdes.configure(AvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }

    public Serde<dev.data_for_history.api_property.Value> DfhApiPropertyValue() {
        AvroKafkaSerializer<dev.data_for_history.api_property.Value> serializer = new AvroKafkaSerializer<>();
        AvroKafkaDeserializer<dev.data_for_history.api_property.Value> deserializer = new AvroKafkaDeserializer<>();
        Serde<dev.data_for_history.api_property.Value> serdes = Serdes.serdeFrom(serializer, deserializer);
        serdes.configure(AvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<dev.projects.project.Key> ProProjectKey() {
        AvroKafkaSerializer<dev.projects.project.Key> serializer = new AvroKafkaSerializer<>();
        AvroKafkaDeserializer<dev.projects.project.Key> deserializer = new AvroKafkaDeserializer<>();
        Serde<dev.projects.project.Key> serdes = Serdes.serdeFrom(serializer, deserializer);
        serdes.configure(AvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }

    public Serde<dev.projects.project.Value> ProProjectValue() {
        AvroKafkaSerializer<dev.projects.project.Value> serializer = new AvroKafkaSerializer<>();
        AvroKafkaDeserializer<dev.projects.project.Value> deserializer = new AvroKafkaDeserializer<>();
        Serde<dev.projects.project.Value> serdes = Serdes.serdeFrom(serializer, deserializer);
        serdes.configure(AvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<dev.system.config.Key> SysConfigKey() {
        AvroKafkaSerializer<dev.system.config.Key> serializer = new AvroKafkaSerializer<>();
        AvroKafkaDeserializer<dev.system.config.Key> deserializer = new AvroKafkaDeserializer<>();
        Serde<dev.system.config.Key> serdes = Serdes.serdeFrom(serializer, deserializer);
        serdes.configure(AvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }

    public Serde<dev.system.config.Value> SysConfigValue() {
        AvroKafkaSerializer<dev.system.config.Value> serializer = new AvroKafkaSerializer<>();
        AvroKafkaDeserializer<dev.system.config.Value> deserializer = new AvroKafkaDeserializer<>();
        Serde<dev.system.config.Value> serdes = Serdes.serdeFrom(serializer, deserializer);
        serdes.configure(AvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }


    public Serde<ProjectProfileKey> ProjectProfileKey() {
        AvroKafkaSerializer<ProjectProfileKey> serializer = new AvroKafkaSerializer<>();
        AvroKafkaDeserializer<ProjectProfileKey> deserializer = new AvroKafkaDeserializer<>();
        Serde<ProjectProfileKey> serdes = Serdes.serdeFrom(serializer, deserializer);
        serdes.configure(AvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }

    public Serde<ProjectProfileValue> ProjectProfileValue() {
        AvroKafkaSerializer<ProjectProfileValue> serializer = new AvroKafkaSerializer<>();
        AvroKafkaDeserializer<ProjectProfileValue> deserializer = new AvroKafkaDeserializer<>();
        Serde<ProjectProfileValue> serdes = Serdes.serdeFrom(serializer, deserializer);
        serdes.configure(AvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<ProjectPropertyKey> ProjectPropertyKey() {
        AvroKafkaSerializer<ProjectPropertyKey> serializer = new AvroKafkaSerializer<>();
        AvroKafkaDeserializer<ProjectPropertyKey> deserializer = new AvroKafkaDeserializer<>();
        Serde<ProjectPropertyKey> serdes = Serdes.serdeFrom(serializer, deserializer);
        serdes.configure(AvroSerdesConfig.INSTANCE.getConfig(), true);

        return serdes;
    }

    public Serde<ProjectPropertyValue> ProjectPropertyValue() {
        AvroKafkaSerializer<ProjectPropertyValue> serializer = new AvroKafkaSerializer<>();
        AvroKafkaDeserializer<ProjectPropertyValue> deserializer = new AvroKafkaDeserializer<>();
        Serde<ProjectPropertyValue> serdes = Serdes.serdeFrom(serializer, deserializer);
        serdes.configure(AvroSerdesConfig.INSTANCE.getConfig(), false);

        return serdes;
    }

    public Serde<BooleanMap> BooleanMapValue() {
        AvroKafkaSerializer<BooleanMap> serializer = new AvroKafkaSerializer<>();
        AvroKafkaDeserializer<BooleanMap> deserializer = new AvroKafkaDeserializer<>();
        Serde<BooleanMap> serdes = Serdes.serdeFrom(serializer, deserializer);
        serdes.configure(AvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<ProfileProperty> ProfilePropertyValue() {
        AvroKafkaSerializer<ProfileProperty> serializer = new AvroKafkaSerializer<>();
        AvroKafkaDeserializer<ProfileProperty> deserializer = new AvroKafkaDeserializer<>();
        Serde<ProfileProperty> serdes = Serdes.serdeFrom(serializer, deserializer);
        serdes.configure(AvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }

    public Serde<ProfilePropertyMap> ProfilePropertyMapValue() {
        AvroKafkaSerializer<ProfilePropertyMap> serializer = new AvroKafkaSerializer<>();
        AvroKafkaDeserializer<ProfilePropertyMap> deserializer = new AvroKafkaDeserializer<>();
        Serde<ProfilePropertyMap> serdes = Serdes.serdeFrom(serializer, deserializer);
        serdes.configure(AvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }
}
