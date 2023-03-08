package org.geovistory.toolbox.streams.rdf.processors.project;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.geovistory.toolbox.streams.avro.ProjectRdfKey;
import org.geovistory.toolbox.streams.avro.ProjectRdfValue;

public record ProjectRdfReturnValue(StreamsBuilder builder,
                                    KStream<ProjectRdfKey, ProjectRdfValue> projectRdfStream) {
}
