package org.geovistory.toolbox.streams.entity.label.processsors.community;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.geovistory.toolbox.streams.avro.CommunityTopStatementsKey;
import org.geovistory.toolbox.streams.avro.CommunityTopStatementsValue;

public record CommunityTopStatementsReturnValue(StreamsBuilder builder,
                                                KTable<CommunityTopStatementsKey, CommunityTopStatementsValue> communityTopStatementTable,
                                                KStream<CommunityTopStatementsKey, CommunityTopStatementsValue> communityTopStatementStream) {
}
