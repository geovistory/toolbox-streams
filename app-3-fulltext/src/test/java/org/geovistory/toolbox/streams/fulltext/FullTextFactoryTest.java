package org.geovistory.toolbox.streams.fulltext;

import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.fulltext.processors.FullTextFactory;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class FullTextFactoryTest {
    @Test
    void testCreateFulltextMethod() {
        var propIdSecondPart = 3;
        var propIdFirstPart = 4;
        var labelConfig = EntityLabelConfig.newBuilder().setLabelParts(List.of(
                // second part
                EntityLabelConfigPart.newBuilder().setOrdNum(2).setField(EntityLabelConfigPartField.newBuilder()
                        .setFkProperty(propIdSecondPart)
                        .setIsOutgoing(true)
                        .setNrOfStatementsInLabel(2).build()).build(),
                // first part
                EntityLabelConfigPart.newBuilder().setOrdNum(1).setField(EntityLabelConfigPartField.newBuilder()
                        .setFkProperty(propIdFirstPart)
                        .setIsOutgoing(false)
                        .setNrOfStatementsInLabel(1).build()).build()
        )).build();

        var value = EntityFieldTextMapWithConfigValue.newBuilder()
                .setLabelConfig(labelConfig)
                .build();

        var map = value.getFields();

        var in = FieldLabelWithTopLabelsValue.newBuilder()
                .setPropertyId(propIdFirstPart)
                .setIsOutgoing(false)
                .setPropertyLabel("has friend")
                .setTargetLabels(List.of("Max", "Mia"))
                .build();

        var out = FieldLabelWithTopLabelsValue.newBuilder()
                .setPropertyId(propIdSecondPart)
                .setIsOutgoing(true)
                .setPropertyLabel("participates in")
                .setTargetLabels(List.of("Voyage 1", "Voyage 2"))
                .build();

        var out2 = FieldLabelWithTopLabelsValue.newBuilder()
                .setPropertyId(9876543)
                .setIsOutgoing(true)
                .setPropertyLabel("has fun with")
                .setTargetLabels(List.of("Toy 1", "Toy 2"))
                .build();


        map.put(FullTextFactory.getFieldKey(false, propIdFirstPart), in);

        map.put(FullTextFactory.getFieldKey(true, propIdSecondPart), out);

        map.put(FullTextFactory.getFieldKey(true, 9876543), out2);


        var result = FullTextFactory.createFulltext(value);
        assertThat(result).isEqualTo("has friend: Max, Mia.\nparticipates in: Voyage 1, Voyage 2.\nhas fun with: Toy 1, Toy 2.");
    }


    @Test
    void testCreateFulltextMethodWithEmptyLabels() {
        var propIdSecondPart = 3;
        var propIdFirstPart = 4;
        var labelConfig = EntityLabelConfig.newBuilder().setLabelParts(List.of(
                // second part
                EntityLabelConfigPart.newBuilder().setOrdNum(2).setField(EntityLabelConfigPartField.newBuilder()
                        .setFkProperty(propIdSecondPart)
                        .setIsOutgoing(true)
                        .setNrOfStatementsInLabel(2).build()).build(),
                // first part
                EntityLabelConfigPart.newBuilder().setOrdNum(1).setField(EntityLabelConfigPartField.newBuilder()
                        .setFkProperty(propIdFirstPart)
                        .setIsOutgoing(false)
                        .setNrOfStatementsInLabel(1).build()).build()
        )).build();

        var value = EntityFieldTextMapWithConfigValue.newBuilder()
                .setLabelConfig(labelConfig)
                .build();

        var map = value.getFields();

        var in = FieldLabelWithTopLabelsValue.newBuilder()
                .setPropertyId(propIdFirstPart)
                .setIsOutgoing(false)
                .setPropertyLabel("has friend")
                .setTargetLabels(List.of("", ""))
                .build();


        map.put(FullTextFactory.getFieldKey(false, propIdFirstPart), in);


        var result = FullTextFactory.createFulltext(value);
        assertThat(result).isEqualTo("");
    }

}
