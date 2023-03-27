package org.geovistory.toolbox.streams.statement.object;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.geovistory.toolbox.streams.avro.NodeKey;
import org.geovistory.toolbox.streams.avro.NodeValue;
import org.geovistory.toolbox.streams.avro.StatementEnrichedValue;
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

    public KTable<dev.information.statement.Key, StatementEnrichedValue> statementWithSubjectTable() {
        return getTable(
                Env.INSTANCE.TOPIC_STATEMENT_WITH_SUBJECT,
                avroSerdes.InfStatementKey(),
                avroSerdes.StatementEnrichedValue()
        );
    }


    public KTable<NodeKey, NodeValue> nodeTable() {
        return getTable(
                Env.INSTANCE.TOPIC_NODES,
                avroSerdes.NodeKey(),
                avroSerdes.NodeValue()
        );
    }

}