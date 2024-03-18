package org.geovistory.toolbox.streams.entity.label3.processors;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.geovistory.toolbox.streams.avro.EdgeValue;
import org.geovistory.toolbox.streams.avro.LabelEdge;
import org.geovistory.toolbox.streams.avro.NodeValue;
import org.geovistory.toolbox.streams.entity.label3.names.Sink;

import static org.geovistory.toolbox.streams.lib.Utils.getLanguageFromId;

public class CreateLabelEdges implements Processor<String, EdgeValue, String, LabelEdge> {
    private ProcessorContext<String, LabelEdge> context;

    public void init(ProcessorContext<String, LabelEdge> context) {
        this.context = context;
    }

    public void process(Record<String, EdgeValue> record) {
        var inVal = record.value();
        var newVal = LabelEdge.newBuilder()
                .setProjectId(inVal.getProjectId())
                .setSourceClassId(inVal.getSourceEntity().getFkClass())
                .setSourceId(inVal.getSourceId())
                .setPropertyId(inVal.getPropertyId())
                .setIsOutgoing(inVal.getIsOutgoing())
                .setOrdNum(inVal.getOrdNum())
                .setModifiedAt(inVal.getModifiedAt())
                .setTargetId(inVal.getTargetId())
                .setTargetLabel(inVal.getTargetNode().getLabel())
                .setTargetLabelLanguage(extractLabelLanguage(inVal.getTargetNode()))
                .setTargetIsInProject(inVal.getTargetProjectEntity() != null)
                .setDeleted(inVal.getDeleted())
                .build();

        var targetIsLiteral = record.value().getTargetNode().getEntity() == null;
        if (targetIsLiteral) this.context.forward(record.withValue(newVal), Sink.LABEL_EDGE_BY_SOURCE);
        else this.context.forward(record.withValue(newVal), Sink.LABEL_EDGE_BY_TARGET);
    }

    private static String extractLabelLanguage(NodeValue n) {
        if (n.getLangString() != null) {
            var langCode = getLanguageFromId(n.getLangString().getFkLanguage());
            if (langCode != null) return langCode;
        }
        return "unknown";
    }


}