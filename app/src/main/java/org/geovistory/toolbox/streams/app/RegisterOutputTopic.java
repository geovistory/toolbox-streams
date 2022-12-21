package org.geovistory.toolbox.streams.app;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.topologies.GeovClassLabel;
import org.geovistory.toolbox.streams.topologies.OntomeClassLabel;
import org.geovistory.toolbox.streams.topologies.ProjectClass;
import org.geovistory.toolbox.streams.topologies.ProjectProfiles;

/**
 * This class provides helper methods to register
 * output topics (generated by this app).
 * These helper methods are mainly used for testing.
 */
public class RegisterOutputTopic {
    public StreamsBuilder builder;
    public ConfluentAvroSerdes avroSerdes;

    public RegisterOutputTopic(StreamsBuilder builder) {
        this.builder = builder;
        this.avroSerdes = new ConfluentAvroSerdes();
    }

    public KStream<ProjectProfileKey, ProjectProfileValue> projectProfileStream() {
        return builder.stream(ProjectProfiles.output.TOPICS.project_profile,
                Consumed.with(avroSerdes.ProjectProfileKey(), avroSerdes.ProjectProfileValue()));
    }

    public KStream<OntomeClassLabelKey, OntomeClassLabelValue> ontomeClassLabelStream() {
        return builder.stream(OntomeClassLabel.output.TOPICS.ontome_class_label,
                Consumed.with(avroSerdes.OntomeClassLabelKey(), avroSerdes.OntomeClassLabelValue()));
    }

    public KStream<GeovClassLabelKey, GeovClassLabelValue> geovClassLabelStream() {
        return builder.stream(GeovClassLabel.output.TOPICS.geov_class_label,
                Consumed.with(avroSerdes.GeovClassLabelKey(), avroSerdes.GeovClassLabelValue()));
    }

    public KStream<ProjectClassKey, ProjectClassValue> projectClassStream() {
        return builder.stream(ProjectClass.output.TOPICS.project_class,
                Consumed.with(avroSerdes.ProjectClassKey(), avroSerdes.ProjectClassValue()));

    }
}