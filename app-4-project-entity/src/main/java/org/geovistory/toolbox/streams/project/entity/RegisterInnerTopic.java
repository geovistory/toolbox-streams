
package org.geovistory.toolbox.streams.project.entity;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.geovistory.toolbox.streams.avro.ProjectEntityKey;
import org.geovistory.toolbox.streams.avro.ProjectEntityTopStatementsValue;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.InputTopicHelper;
import org.geovistory.toolbox.streams.project.entity.topologies.ProjectEntityTopStatements;

/**
 * This class provides helper methods to register
 * output topics (generated by this app).
 * These helper methods are mainly used for testing.
 */
public class RegisterInnerTopic extends InputTopicHelper {
    public ConfluentAvroSerdes avroSerdes;

    public RegisterInnerTopic(StreamsBuilder builder) {
        super(builder);
        this.avroSerdes = new ConfluentAvroSerdes();
    }


    public KTable<ProjectEntityKey, ProjectEntityTopStatementsValue> projectEntityTopStatementsTable() {
        return getTable(ProjectEntityTopStatements.output.TOPICS.project_entity_top_statements,
                avroSerdes.ProjectEntityKey(), avroSerdes.ProjectEntityTopStatementsValue());
    }
    public KStream<ProjectEntityKey, ProjectEntityTopStatementsValue> projectEntityTopStatementsStream() {
        return getStream(ProjectEntityTopStatements.output.TOPICS.project_entity_top_statements,
                avroSerdes.ProjectEntityKey(), avroSerdes.ProjectEntityTopStatementsValue());
    }

}