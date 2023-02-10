package org.geovistory.toolbox.streams.project.entity.label.processors;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.geovistory.toolbox.streams.avro.ProjectEntityKey;
import org.geovistory.toolbox.streams.avro.ProjectEntityTopStatementsValue;

public record ProjectEntityTopStatementsReturnValue(StreamsBuilder builder,
                                                    KTable<ProjectEntityKey, ProjectEntityTopStatementsValue> projectEntityTopStatementTable,
                                                    KStream<ProjectEntityKey, ProjectEntityTopStatementsValue> projectEntityTopStatementStream) {
}
