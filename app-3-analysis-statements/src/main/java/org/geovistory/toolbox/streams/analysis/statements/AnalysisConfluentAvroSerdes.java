package org.geovistory.toolbox.streams.analysis.statements;


import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.geovistory.toolbox.streams.analysis.statements.avro.AnalysisStatementValue;
import org.geovistory.toolbox.streams.avro.AnalysisStatementKey;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdesConfig;

public class AnalysisConfluentAvroSerdes {

    public Serde<AnalysisStatementKey> AnalysisStatementKey() {
        Serde<AnalysisStatementKey> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), true);
        return serdes;
    }

    public Serde<AnalysisStatementValue> AnalysisStatementValue() {
        Serde<AnalysisStatementValue> serdes = new SpecificAvroSerde<>();
        serdes.configure(ConfluentAvroSerdesConfig.INSTANCE.getConfig(), false);
        return serdes;
    }
}
