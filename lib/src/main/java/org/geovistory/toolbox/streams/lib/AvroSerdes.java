package org.geovistory.toolbox.streams.lib;

import io.apicurio.registry.rest.client.request.Parameters;
import io.apicurio.registry.serde.SerdeConfig;
import io.apicurio.registry.serde.avro.AvroKafkaDeserializer;
import io.apicurio.registry.serde.avro.AvroKafkaSerdeConfig;
import io.apicurio.registry.serde.avro.AvroKafkaSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.geovistory.toolbox.streams.avro.Id;
import org.geovistory.toolbox.streams.avro.ProfileIds;
import org.geovistory.toolbox.streams.avro.ProjectPropertyKey;

import java.util.HashMap;


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


    public static Serde<dev.data_for_history.api_property.Key> DfhApiPropertyKey() {
        AvroKafkaSerializer<dev.data_for_history.api_property.Key> serializer = new AvroKafkaSerializer<>();
        AvroKafkaDeserializer<dev.data_for_history.api_property.Key> deserializer = new AvroKafkaDeserializer<>();
        Serde<dev.data_for_history.api_property.Key> serdes = Serdes.serdeFrom(serializer, deserializer);
        serdes.configure(AvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }

    public static Serde<dev.data_for_history.api_property.Value> DfhApiPropertyValue() {
        AvroKafkaSerializer<dev.data_for_history.api_property.Value> serializer = new AvroKafkaSerializer<>();
        AvroKafkaDeserializer<dev.data_for_history.api_property.Value> deserializer = new AvroKafkaDeserializer<>();
        Serde<dev.data_for_history.api_property.Value> serdes = Serdes.serdeFrom(serializer, deserializer);
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

    public static Serde<ProfileIds> ProfileIdsValue() {
        AvroKafkaSerializer<ProfileIds> serializer = new AvroKafkaSerializer<>();
        AvroKafkaDeserializer<ProfileIds> deserializer = new AvroKafkaDeserializer<>();
        Serde<ProfileIds> serdes = Serdes.serdeFrom(serializer, deserializer);
        serdes.configure(AvroSerdesConfig.INSTANCE.getConfig(), false);

        return serdes;
    }

    public static Serde<ProjectPropertyKey> ProjectPropertyKeyKey() {
        AvroKafkaSerializer<ProjectPropertyKey> serializer = new AvroKafkaSerializer<>();
        AvroKafkaDeserializer<ProjectPropertyKey> deserializer = new AvroKafkaDeserializer<>();
        Serde<ProjectPropertyKey> serdes = Serdes.serdeFrom(serializer, deserializer);
        serdes.configure(AvroSerdesConfig.INSTANCE.getConfig(), true);

        return serdes;
    }
    public static Serde<ProjectPropertyKey> ProjectPropertyKeyValue() {
        AvroKafkaSerializer<ProjectPropertyKey> serializer = new AvroKafkaSerializer<>();
        AvroKafkaDeserializer<ProjectPropertyKey> deserializer = new AvroKafkaDeserializer<>();
        Serde<ProjectPropertyKey> serdes = Serdes.serdeFrom(serializer, deserializer);
        serdes.configure(AvroSerdesConfig.INSTANCE.getConfig(), false);

        return serdes;
    }

}
