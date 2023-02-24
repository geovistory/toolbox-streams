package org.geovistory.toolbox.streams.entity.processors.community;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.geovistory.toolbox.streams.avro.CommunityEntityClassMetadataValue;
import org.geovistory.toolbox.streams.avro.CommunityEntityKey;

public record CommunityEntityClassMetadataReturnValue(StreamsBuilder builder,

                                                      KTable<CommunityEntityKey, CommunityEntityClassMetadataValue> projectEntityClassMetadataTable,
                                                      KStream<CommunityEntityKey, CommunityEntityClassMetadataValue> projectEntityClassMetadataStream) {
}
