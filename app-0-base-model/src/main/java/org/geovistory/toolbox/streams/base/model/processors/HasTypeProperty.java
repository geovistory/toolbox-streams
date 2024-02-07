package org.geovistory.toolbox.streams.base.model.processors;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.base.model.*;
import org.geovistory.toolbox.streams.lib.ConfiguredAvroSerde;
import org.geovistory.toolbox.streams.lib.IdenticalRecordsFilterSupplier;
import org.geovistory.toolbox.streams.lib.TopicNameEnum;
import org.geovistory.toolbox.streams.lib.Utils;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.LinkedList;
import java.util.List;

@ApplicationScoped
public class HasTypeProperty {

    @Inject
    AvroSerdes avroSerdes;

    @Inject
    ConfiguredAvroSerde as;

    @ConfigProperty(name = "ts.input.topic.name.prefix", defaultValue = "")
    String inPrefix;
    @Inject
    BuilderSingleton builderSingleton;

    @Inject
    InputTopicNames inputTopicNames;

    @Inject
    OutputTopicNames outputTopicNames;

    public HasTypeProperty(BuilderSingleton builderSingleton, InputTopicNames inputTopicNames, OutputTopicNames outputTopicNames) {
        this.builderSingleton = builderSingleton;
        this.inputTopicNames = inputTopicNames;
        this.outputTopicNames = outputTopicNames;
    }


    public void addProcessorsStandalone() {
        addProcessors(
                new OntomePropertyProjected().getRegistrar(
                        this.avroSerdes, this.builderSingleton, this.inputTopicNames, this.outputTopicNames
                ).kStream
        );
    }


    public HasTypePropertyReturnValue addProcessors(
            KStream<OntomePropertyKey, OntomePropertyValue> apiPropertyStream
    ) {

        /* STREAM PROCESSORS */
        // 2)
        // Filter has type sub-properties
        var hasTypeStream = apiPropertyStream
                .flatMapValues(
                        (key, value) -> {
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
                        },
                        Named.as("kstream-flat-map-values-ontome-properties-to-has-type-properties")
                );

        var groupedByDomain = hasTypeStream.groupBy(
                (key, value) -> HasTypePropertyKey.newBuilder()
                        .setClassId(value.getClassId()).build(),
                Grouped.with(
                        inner.TOPICS.has_type_properties_grouped,
                        as.key(),
                        as.value()
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
                        .withKeySerde(as.key())
                        .withValueSerde(as.value()));

        var hasTypePropertyStream = hasTypePropertyTable
                .toStream(
                        Named.as(inner.TOPICS.has_type_properties_aggregated + "-to-stream")
                )
                .mapValues(
                        (readOnlyKey, value) -> {
                            // if all are deleted, mark as deleted
                            var deleted = !value.getDeletedMap().getItem().containsValue(false);
                            return HasTypePropertyValue.newBuilder()
                                    .setClassId(value.getClassId())
                                    .setPropertyId(value.getPropertyId())
                                    .setDeleted$1(deleted)
                                    .build();
                        },
                        Named.as("kstream-mapvalues-mark-has-type-property-as-deleted")
                )
                .transform(new IdenticalRecordsFilterSupplier<>(
                                "has_type_property_suppress_duplicates",
                                as.key(),
                                as.value()),
                        Named.as("has_type_property_suppress_duplicates"));

        /* SINK PROCESSORS */
        hasTypePropertyStream
                .to(
                        outputTopicNames.hasTypeProperty() + "-producer",
                        Produced.with(as.key(), as.value())
                );


        return new HasTypePropertyReturnValue(hasTypePropertyStream);

    }

    public static boolean isHasTypeProperty(OntomePropertyValue property) {
        if (property.getDfhPkProperty() == Prop.HAS_TYPE.get()) return true;
        else if (property.getDfhParentProperties() != null
                && property.getDfhParentProperties().contains(Prop.HAS_TYPE.get())
        ) return true;
        else return property.getDfhAncestorProperties() != null
                    && property.getDfhAncestorProperties().contains(Prop.HAS_TYPE.get());
    }


    public String inApiProperty() {
        return Utils.prefixedIn(inPrefix, TopicNameEnum.dfh_api_property.getValue());
    }


    public enum inner {
        TOPICS;
        public final String has_type_properties_grouped = "has_type_properties_grouped";
        public final String has_type_properties_aggregated = "has_type_properties_aggregated";


    }


}
