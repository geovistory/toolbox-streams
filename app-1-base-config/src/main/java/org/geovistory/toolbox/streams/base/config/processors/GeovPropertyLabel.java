package org.geovistory.toolbox.streams.base.config.processors;

import ts.projects.text_property.Key;
import ts.projects.text_property.Value;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.geovistory.toolbox.streams.avro.GeovPropertyLabelKey;
import org.geovistory.toolbox.streams.avro.GeovPropertyLabelValue;
import org.geovistory.toolbox.streams.base.config.AvroSerdes;
import org.geovistory.toolbox.streams.base.config.OutputTopicNames;
import org.geovistory.toolbox.streams.base.config.RegisterInnerTopic;
import org.geovistory.toolbox.streams.base.config.RegisterInputTopic;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

@ApplicationScoped
public class GeovPropertyLabel {
    @Inject
    AvroSerdes avroSerdes;

    @Inject
    RegisterInputTopic registerInputTopic;

    @Inject
    RegisterInnerTopic registerInnerTopic;

    @Inject
    OutputTopicNames outputTopicNames;

    public GeovPropertyLabel(AvroSerdes avroSerdes, RegisterInputTopic registerInputTopic, RegisterInnerTopic registerInnerTopic, OutputTopicNames outputTopicNames) {
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

    public GeovPropertyLabelReturnValue addProcessors(
            KStream<Key, Value> proTextPropertyStream
    ) {

        /* STREAM PROCESSORS */

        // 2)
        var geovPropertyLabel = proTextPropertyStream
                .flatMap(
                        (key, value) -> {
                            List<KeyValue<GeovPropertyLabelKey, GeovPropertyLabelValue>> result = new LinkedList<>();
                            var propertyId = value.getFkDfhProperty();
                            var domainId = value.getFkDfhPropertyDomain();
                            var rangeId = value.getFkDfhPropertyRange();

                            // validate
                            if (propertyId == null) return result;
                            if (value.getFkDfhProperty() == null) return result;
                            if (domainId == null && rangeId == null) return result;

                            int classId = domainId != null ? domainId : rangeId;
                            var isOutgoing = domainId != null;

                            var k = GeovPropertyLabelKey.newBuilder()
                                    .setProjectId(value.getFkProject())
                                    .setClassId(classId)
                                    .setIsOutgoing(isOutgoing)
                                    .setPropertyId(value.getFkDfhProperty())
                                    .setLanguageId(value.getFkLanguage())
                                    .build();
                            var v = GeovPropertyLabelValue.newBuilder()
                                    .setProjectId(value.getFkProject())
                                    .setProjectId(value.getFkProject())
                                    .setClassId(classId)
                                    .setIsOutgoing(isOutgoing)
                                    .setPropertyId(value.getFkDfhProperty())
                                    .setLanguageId(value.getFkLanguage())
                                    .setLabel(value.getString())
                                    .setDeleted$1(Objects.equals(value.getDeleted$1(), "true"))
                                    .build();

                            result.add(KeyValue.pair(k, v));
                            return result;
                        },
                        Named.as("kstream-flatmap-pro-text-property-to-geov-property-label")
                );

        /* SINK PROCESSORS */
        geovPropertyLabel
                .to(
                        outputTopicNames.geovPropertyLabel(),
                        Produced.with(avroSerdes.GeovPropertyLabelKey(), avroSerdes.GeovPropertyLabelValue())
                );

        return new GeovPropertyLabelReturnValue(geovPropertyLabel);

    }


}
