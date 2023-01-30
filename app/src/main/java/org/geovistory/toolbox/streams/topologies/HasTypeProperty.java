package org.geovistory.toolbox.streams.topologies;

import dev.data_for_history.api_property.Key;
import dev.data_for_history.api_property.Value;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.app.DbTopicNames;
import org.geovistory.toolbox.streams.app.Prop;
import org.geovistory.toolbox.streams.app.RegisterInputTopic;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.Utils;

import java.util.LinkedList;
import java.util.List;


public class HasTypeProperty {

    public static void main(String[] args) {
        System.out.println(buildStandalone(new StreamsBuilder()).describe());
    }

    public static Topology buildStandalone(StreamsBuilder builder) {

        var register = new RegisterInputTopic(builder);

        var apiPropertyStream = register.dfhApiPropertyStream();

        return addProcessors(builder, apiPropertyStream).builder().build();

    }

    public static HasTypePropertyReturnValue addProcessors(
            StreamsBuilder builder,
            KStream<Key, Value> apiPropertyStream
    ) {

        var avroSerdes = new ConfluentAvroSerdes();

        /* STREAM PROCESSORS */
        // 2)
        // Filter has type sub-properties
        var hasTypeStream = apiPropertyStream
                .flatMapValues((key, value) -> {
                    List<HasTypePropertyGroupByValue> result = new LinkedList<>();

                    if (isHasTypeProperty(value)) {
                        var deleted = Utils.booleanIsEqualTrue(value.getRemovedFromApi()) ||
                                Utils.stringIsEqualTrue(value.getDeleted$1());
                        result.add(HasTypePropertyGroupByValue.newBuilder()
                                .setPropertyId(value.getDfhPkProperty())
                                .setClassId(value.getDfhPropertyDomain())
                                .setProfileId(value.getDfhFkProfile())
                                .setDeleted(deleted)
                                .build());
                    }

                    return result;
                });

        var groupedByDomain = hasTypeStream.groupBy(
                (key, value) -> HasTypePropertyKey.newBuilder()
                        .setClassId(value.getClassId()).build(),
                Grouped.with(
                        inner.TOPICS.has_type_properties_grouped,
                        avroSerdes.HasTypePropertyKey(),
                        avroSerdes.HasTypePropertyGroupByValue()
                )
        );
        var hasTypePropertyTable = groupedByDomain.aggregate(() -> HasTypePropertyAggregateValue.newBuilder()
                        .setPropertyId(0)
                        .setClassId(0)
                        .setDeletedMap(BooleanMap.newBuilder().build())
                        .build(),
                (key, value, aggregate) -> {
                    aggregate.setPropertyId(value.getPropertyId());
                    aggregate.setClassId(value.getClassId());

                    var map = aggregate.getDeletedMap().getItem();
                    var profileIdString = value.getProfileId() + "";
                    map.put(profileIdString, value.getDeleted());


                    return aggregate;
                },
                Materialized.<HasTypePropertyKey, HasTypePropertyAggregateValue, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.has_type_properties_aggregated)
                        .withKeySerde(avroSerdes.HasTypePropertyKey())
                        .withValueSerde(avroSerdes.HasTypePropertyAggregateValue()));

        var hasTypePropertyStream = hasTypePropertyTable
                .toStream()
                .mapValues((readOnlyKey, value) -> {
                            // if all are deleted, mark as deleted
                            var deleted = !value.getDeletedMap().getItem().containsValue(false);
                            return HasTypePropertyValue.newBuilder()
                                    .setClassId(value.getClassId())
                                    .setPropertyId(value.getPropertyId())
                                    .setDeleted$1(deleted)
                                    .build();
                        }
                );

        /* SINK PROCESSORS */
        hasTypePropertyStream
                .to(
                        output.TOPICS.has_type_property,
                        Produced.with(avroSerdes.HasTypePropertyKey(), avroSerdes.HasTypePropertyValue())
                );


        return new HasTypePropertyReturnValue(builder, hasTypePropertyStream);

    }

    public static boolean isHasTypeProperty(Value property) {
        if (property.getDfhPkProperty() == Prop.HAS_TYPE.get()) return true;
        else if (property.getDfhParentProperties() != null
                && property.getDfhParentProperties().contains(Prop.HAS_TYPE.get())
        ) return true;
        else return property.getDfhAncestorProperties() != null
                    && property.getDfhAncestorProperties().contains(Prop.HAS_TYPE.get());
    }

    public enum input {
        TOPICS;
        public final String api_property = DbTopicNames.dfh_api_property.getName();


    }

    public enum inner {
        TOPICS;
        public final String has_type_properties_grouped = "has_type_properties_grouped";
        public final String has_type_properties_aggregated = "has_type_properties_aggregated";


    }


    public enum output {
        TOPICS;
        public final String has_type_property = Utils.tsPrefixed("has_type_property");

    }

}
