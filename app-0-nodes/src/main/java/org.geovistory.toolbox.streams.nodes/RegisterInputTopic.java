package org.geovistory.toolbox.streams.nodes;

import org.apache.kafka.streams.kstream.KStream;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.lib.TopicNameEnum;
import org.geovistory.toolbox.streams.lib.TsRegisterInputTopic;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

/**
 * This class provides helper methods to register
 * source topics (topics consumed but not generated by this app)
 */

@ApplicationScoped
public class RegisterInputTopic extends TsRegisterInputTopic {

    @Inject
    AvroSerdes avroSerdes;
    @Inject
    public BuilderSingleton builderSingleton;

    @ConfigProperty(name = "ts.input.topic.name.prefix", defaultValue = "")
    String prefix;

    RegisterInputTopic() {
    }

    public RegisterInputTopic(AvroSerdes avroSerdes, BuilderSingleton builderSingleton) {
        this.avroSerdes = avroSerdes;
        this.builderSingleton = builderSingleton;
    }


    public KStream<dev.information.resource.Key, dev.information.resource.Value> infResourceStream() {
        return getStream(
                builderSingleton.builder,
                prefixedIn(prefix, TopicNameEnum.inf_resource.getValue()),
                avroSerdes.InfResourceKey(),
                avroSerdes.InfResourceValue()
        );
    }


    public KStream<dev.information.language.Key, dev.information.language.Value> infLanguageStream() {
        return getStream(
                builderSingleton.builder,
                prefixedIn(prefix, TopicNameEnum.inf_language.getValue()),
                avroSerdes.InfLanguageKey(),
                avroSerdes.InfLanguageValue()
        );
    }

    public KStream<dev.information.appellation.Key, dev.information.appellation.Value> infAppellationStream() {
        return getStream(
                builderSingleton.builder,
                prefixedIn(prefix, TopicNameEnum.inf_appellation.getValue()),
                avroSerdes.InfAppellationKey(),
                avroSerdes.InfAppellationValue()
        );
    }

    public KStream<dev.information.lang_string.Key, dev.information.lang_string.Value> infLangStringStream() {
        return getStream(
                builderSingleton.builder,
                prefixedIn(prefix, TopicNameEnum.inf_lang_string.getValue()),
                avroSerdes.InfLangStringKey(),
                avroSerdes.InfLangStringValue()
        );
    }

    public KStream<dev.information.place.Key, dev.information.place.Value> infPlaceStream() {
        return getStream(
                builderSingleton.builder,
                prefixedIn(prefix, TopicNameEnum.inf_place.getValue()),
                avroSerdes.InfPlaceKey(),
                avroSerdes.InfPlaceValue()
        );
    }

    public KStream<dev.information.time_primitive.Key, dev.information.time_primitive.Value> infTimePrimitiveStream() {
        return getStream(
                builderSingleton.builder,
                prefixedIn(prefix, TopicNameEnum.inf_time_primitive.getValue()),
                avroSerdes.InfTimePrimitiveKey(),
                avroSerdes.InfTimePrimitiveValue()
        );
    }

    public KStream<dev.information.dimension.Key, dev.information.dimension.Value> infDimensionStream() {
        return getStream(
                builderSingleton.builder,
                prefixedIn(prefix, TopicNameEnum.inf_dimension.getValue()),
                avroSerdes.InfDimensionKey(),
                avroSerdes.InfDimensionValue()
        );
    }


    public KStream<dev.data.digital.Key, dev.data.digital.Value> datDigitalStream() {
        return getStream(
                builderSingleton.builder,
                prefixedIn(prefix, TopicNameEnum.dat_digital.getValue()),
                avroSerdes.DatDigitalKey(),
                avroSerdes.DatDigitalValue()
        );
    }

    public KStream<dev.tables.cell.Key, dev.tables.cell.Value> tabCellStream() {
        return getStream(
                builderSingleton.builder,
                prefixedIn(prefix, TopicNameEnum.tab_cell.getValue()),
                avroSerdes.TabCellKey(),
                avroSerdes.TabCellValue()
        );
    }

}