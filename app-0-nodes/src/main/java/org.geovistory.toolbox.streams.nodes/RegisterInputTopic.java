package org.geovistory.toolbox.streams.nodes;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
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

    public KStream<dev.information.resource.Key, dev.information.resource.Value> infResourceStream() {
        return getStream(
                DbTopicNames.inf_resource.getName(),
                avroSerdes.InfResourceKey(),
                avroSerdes.InfResourceValue()
        );
    }


    public KStream<dev.information.language.Key, dev.information.language.Value> infLanguageStream() {
        return getStream(
                DbTopicNames.inf_language.getName(),
                avroSerdes.InfLanguageKey(),
                avroSerdes.InfLanguageValue()
        );
    }

    public KStream<dev.information.appellation.Key, dev.information.appellation.Value> infAppellationStream() {
        return getStream(
                DbTopicNames.inf_appellation.getName(),
                avroSerdes.InfAppellationKey(),
                avroSerdes.InfAppellationValue()
        );
    }

    public KStream<dev.information.lang_string.Key, dev.information.lang_string.Value> infLangStringStream() {
        return getStream(
                DbTopicNames.inf_lang_string.getName(),
                avroSerdes.InfLangStringKey(),
                avroSerdes.InfLangStringValue()
        );
    }

    public KStream<dev.information.place.Key, dev.information.place.Value> infPlaceStream() {
        return getStream(
                DbTopicNames.inf_place.getName(),
                avroSerdes.InfPlaceKey(),
                avroSerdes.InfPlaceValue()
        );
    }

    public KStream<dev.information.time_primitive.Key, dev.information.time_primitive.Value> infTimePrimitiveStream() {
        return getStream(
                DbTopicNames.inf_time_primitive.getName(),
                avroSerdes.InfTimePrimitiveKey(),
                avroSerdes.InfTimePrimitiveValue()
        );
    }

    public KStream<dev.information.dimension.Key, dev.information.dimension.Value> infDimensionStream() {
        return getStream(
                DbTopicNames.inf_dimension.getName(),
                avroSerdes.InfDimensionKey(),
                avroSerdes.InfDimensionValue()
        );
    }


    public KStream<dev.data.digital.Key, dev.data.digital.Value> datDigitalStream() {
        return getStream(
                DbTopicNames.dat_digital.getName(),
                avroSerdes.DatDigitalKey(),
                avroSerdes.DatDigitalValue()
        );
    }

    public KStream<dev.tables.cell.Key, dev.tables.cell.Value> tabCellStream() {
        return getStream(
                DbTopicNames.tab_cell.getName(),
                avroSerdes.TabCellKey(),
                avroSerdes.TabCellValue()
        );
    }

}