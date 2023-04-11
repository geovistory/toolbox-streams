package org.geovistory.toolbox.streams.base.config.processors;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.geovistory.toolbox.streams.avro.GeovClassLabelKey;
import org.geovistory.toolbox.streams.avro.GeovClassLabelValue;
import org.geovistory.toolbox.streams.base.config.AvroSerdes;
import org.geovistory.toolbox.streams.base.config.OutputTopicNames;
import org.geovistory.toolbox.streams.base.config.RegisterInnerTopic;
import org.geovistory.toolbox.streams.base.config.RegisterInputTopic;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

@ApplicationScoped
public class GeovClassLabel {
    @Inject
    AvroSerdes avroSerdes;

    @Inject
    RegisterInputTopic registerInputTopic;

    @Inject
    RegisterInnerTopic registerInnerTopic;

    @Inject
    OutputTopicNames outputTopicNames;

    public GeovClassLabel(AvroSerdes avroSerdes, RegisterInputTopic registerInputTopic, RegisterInnerTopic registerInnerTopic, OutputTopicNames outputTopicNames) {
        this.avroSerdes = avroSerdes;
        this.registerInputTopic = registerInputTopic;
        this.registerInnerTopic = registerInnerTopic;
        this.outputTopicNames = outputTopicNames;
    }

    public void addProcessorsStandalone() {
        addProcessors(
                registerInputTopic.proTextPropertyStream()
        );
    }

    public GeovClassLabelReturnValue addProcessors(
            KStream<dev.projects.text_property.Key, dev.projects.text_property.Value> proTextPropertyStream
    ) {

        /* STREAM PROCESSORS */

        // 2)
        var geovClassLabel = proTextPropertyStream
                .flatMap(
                        (key, value) -> {
                            List<KeyValue<GeovClassLabelKey, GeovClassLabelValue>> result = new LinkedList<>();
                            var classId = value.getFkDfhClass();

                            // validate
                            if (classId == null) return result;
                            if (value.getFkSystemType() != 639) return result;

                            var k = GeovClassLabelKey.newBuilder()
                                    .setProjectId(value.getFkProject())
                                    .setClassId(value.getFkDfhClass())
                                    .setLanguageId(value.getFkLanguage())
                                    .build();
                            var v = GeovClassLabelValue.newBuilder()
                                    .setProjectId(value.getFkProject())
                                    .setClassId(value.getFkDfhClass())
                                    .setLanguageId(value.getFkLanguage())
                                    .setLabel(value.getString())
                                    .setDeleted$1(Objects.equals(value.getDeleted$1(), "true"))
                                    .build();

                            result.add(KeyValue.pair(k, v));
                            return result;
                        },
                        Named.as("kstream-flatmap-pro-text-property-to-geov-class-label")
                );

        /* SINK PROCESSORS */
        geovClassLabel
                .to(
                        outputTopicNames.geovClassLabel(),
                        Produced.with(avroSerdes.GeovClassLabelKey(), avroSerdes.GeovClassLabelValue())
                                .withName(outputTopicNames.geovClassLabel() + "-producer")
                );

        return new GeovClassLabelReturnValue(geovClassLabel);

    }


}
