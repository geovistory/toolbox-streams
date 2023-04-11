package org.geovistory.toolbox.streams.entity.label.processsors.project;

import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.geovistory.toolbox.streams.avro.ProjectTopStatementsKey;
import org.geovistory.toolbox.streams.avro.ProjectTopStatementsValue;

public record ProjectTopStatementsReturnValue(
        KTable<ProjectTopStatementsKey, ProjectTopStatementsValue> projectTopStatementTable,
        KStream<ProjectTopStatementsKey, ProjectTopStatementsValue> projectTopStatementStream) {
}
