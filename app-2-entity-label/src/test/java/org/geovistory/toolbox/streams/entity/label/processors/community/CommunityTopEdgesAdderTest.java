package org.geovistory.toolbox.streams.entity.label.processors.community;


import org.geovistory.toolbox.streams.avro.CommunityStatementValue;
import org.geovistory.toolbox.streams.avro.StatementEnrichedValue;
import org.geovistory.toolbox.streams.entity.label.processsors.community.CommunityTopStatementAdder;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import static org.assertj.core.api.Assertions.assertThat;

class CommunityTopEdgesAdderTest {


    @Test
    void testOutgoingOrdering() {
        var s = StatementEnrichedValue.newBuilder()
                .setSubjectId("1")
                .setPropertyId(2)
                .setObjectId("3")
                .build();

        var b = CommunityStatementValue.newBuilder();
        var v0 = new ArrayList<CommunityStatementValue>();
        var v1 = CommunityTopStatementAdder.addStatement(v0,
                b.setProjectCount(1).setStatementId(4).setStatement(s).setAvgOrdNumOfRange(4f).build(), true
        );
        var v2 = CommunityTopStatementAdder.addStatement(v1,
                b.setProjectCount(1).setStatementId(3).setStatement(s).setAvgOrdNumOfRange(3f).build(), true
        );
        var v3 = CommunityTopStatementAdder.addStatement(v2,
                b.setProjectCount(1).setStatementId(2).setStatement(s).setAvgOrdNumOfRange(2f).build(), true
        );
        var v4 = CommunityTopStatementAdder.addStatement(v3,
                b.setProjectCount(1).setStatementId(5).setStatement(s).setAvgOrdNumOfRange(null).build(), true
        );
        var v5 = CommunityTopStatementAdder.addStatement(v4,
                b.setProjectCount(1).setStatementId(0).setStatement(s).setAvgOrdNumOfRange(0f).build(), true
        );
        var v6 = CommunityTopStatementAdder.addStatement(v5,
                b.setProjectCount(1).setStatementId(1).setStatement(s).setAvgOrdNumOfRange(1f).build(), true
        );

        assertThat(v6.size()).isEqualTo(5);
        assertThat(v6.get(0).getStatementId()).isEqualTo(0);
        assertThat(v6.get(3).getStatementId()).isEqualTo(3);
    }

    @Test
    void testOutgoingOrderingByModificationDate() {
        var s = StatementEnrichedValue.newBuilder()
                .setSubjectId("1")
                .setPropertyId(2)
                .setObjectId("3")
                .build();

        var b = CommunityStatementValue.newBuilder();
        var v0 = new ArrayList<CommunityStatementValue>();
        var v1 = CommunityTopStatementAdder.addStatement(v0,
                b.setProjectCount(1).setStatementId(3).setStatement(s).setModifiedAt("2020-03-03T09:25:57.698128Z").build(), true
        );
        var v2 = CommunityTopStatementAdder.addStatement(v1,
                b.setProjectCount(1).setStatementId(4).setStatement(s).setModifiedAt("2020-02-03T09:25:57.698128Z").build(), true
        );
        var v3 = CommunityTopStatementAdder.addStatement(v2,
                b.setProjectCount(1).setStatementId(0).setStatement(s).setModifiedAt("2020-12-03T09:25:57.698128Z").build(), true
        );
        var v4 = CommunityTopStatementAdder.addStatement(v3,
                b.setProjectCount(1).setStatementId(1).setStatement(s).setModifiedAt("2020-11-03T09:25:57.698128Z").build(), true
        );
        var v5 = CommunityTopStatementAdder.addStatement(v4,
                b.setProjectCount(1).setStatementId(2).setStatement(s).setModifiedAt("2020-04-03T09:25:57.698128Z").build(), true
        );
        var v6 = CommunityTopStatementAdder.addStatement(v5,
                b.setProjectCount(1).setStatementId(5).setStatement(s).setModifiedAt("2020-01-03T09:25:57.698128Z").build(), true
        );

        assertThat(v6.size()).isEqualTo(5);
        assertThat(v6.get(0).getStatementId()).isEqualTo(0);
        assertThat(v6.get(1).getStatementId()).isEqualTo(1);
        assertThat(v6.get(2).getStatementId()).isEqualTo(2);
        assertThat(v6.get(3).getStatementId()).isEqualTo(3);
        assertThat(v6.get(4).getStatementId()).isEqualTo(4);
    }

    @Test
    void testIncomingOrderingByModificationDate() {
        var s = StatementEnrichedValue.newBuilder()
                .setSubjectId("1")
                .setPropertyId(2)
                .setObjectId("3")
                .build();

        var b = CommunityStatementValue.newBuilder();
        var v0 = new ArrayList<CommunityStatementValue>();
        var v1 = CommunityTopStatementAdder.addStatement(v0,
                b.setProjectCount(1).setStatementId(3).setStatement(s).setAvgOrdNumOfDomain(null).setModifiedAt("2020-03-03T09:25:57.698128Z").build(),
                false
        );
        var v2 = CommunityTopStatementAdder.addStatement(v1,
                b.setProjectCount(1).setStatementId(4).setStatement(s).setAvgOrdNumOfDomain(null).setModifiedAt("2020-02-03T09:25:57.698128Z").build(),
                false
        );
        var v3 = CommunityTopStatementAdder.addStatement(v2,
                b.setProjectCount(1).setStatementId(0).setStatement(s).setAvgOrdNumOfDomain(null).setModifiedAt("2020-12-03T09:25:57.698128Z").build(),
                false
        );
        var v4 = CommunityTopStatementAdder.addStatement(v3,
                b.setProjectCount(1).setStatementId(1).setStatement(s).setAvgOrdNumOfDomain(3f).setModifiedAt("2020-11-03T09:25:57.698128Z").build(),
                false
        );
        var v5 = CommunityTopStatementAdder.addStatement(v4,
                b.setProjectCount(1).setStatementId(2).setStatement(s).setAvgOrdNumOfDomain(null).setModifiedAt("2020-04-03T09:25:57.698128Z").build(),
                false
        );
        var v6 = CommunityTopStatementAdder.addStatement(v5,
                b.setProjectCount(1).setStatementId(5).setStatement(s).setAvgOrdNumOfDomain(null).setModifiedAt("2020-01-03T09:25:57.698128Z").build(),
                false
        );

        assertThat(v6.size()).isEqualTo(5);
        assertThat(v6.get(0).getStatementId()).isEqualTo(1);
        assertThat(v6.get(1).getStatementId()).isEqualTo(0);
        assertThat(v6.get(2).getStatementId()).isEqualTo(2);
        assertThat(v6.get(3).getStatementId()).isEqualTo(3);
        assertThat(v6.get(4).getStatementId()).isEqualTo(4);
    }

    @Test
    void testIncomingOrderingByStatementId() {
        var s = StatementEnrichedValue.newBuilder()
                .setSubjectId("1")
                .setPropertyId(2)
                .setObjectId("3")
                .build();

        var b = CommunityStatementValue.newBuilder();
        var v0 = new ArrayList<CommunityStatementValue>();
        var v1 = CommunityTopStatementAdder.addStatement(v0,
                b.setProjectCount(1).setStatementId(0).setStatement(s).setAvgOrdNumOfDomain(null).setModifiedAt(null).build(),
                false
        );
        var v2 = CommunityTopStatementAdder.addStatement(v1,
                b.setProjectCount(1).setStatementId(1).setStatement(s).setAvgOrdNumOfDomain(null).setModifiedAt("2020-02-03T09:25:57.698128Z").build(),
                false
        );
        var v3 = CommunityTopStatementAdder.addStatement(v2,
                b.setProjectCount(1).setStatementId(2).setStatement(s).setAvgOrdNumOfDomain(null).setModifiedAt(null).build(),
                false
        );


        assertThat(v3.size()).isEqualTo(3);
        assertThat(v3.get(0).getStatementId()).isEqualTo(1); // because it has a date
        assertThat(v3.get(1).getStatementId()).isEqualTo(2); // because id 2 > 0
        assertThat(v3.get(2).getStatementId()).isEqualTo(0);
    }

    @Test
    void testIncomingOrderingByStatementIdWithSameDate() {
        var s = StatementEnrichedValue.newBuilder()
                .setSubjectId("1")
                .setPropertyId(2)
                .setObjectId("3")
                .build();

        var b = CommunityStatementValue.newBuilder();
        var v0 = new ArrayList<CommunityStatementValue>();
        var v1 = CommunityTopStatementAdder.addStatement(v0,
                b.setProjectCount(1).setStatementId(0).setStatement(s).setAvgOrdNumOfDomain(null).setModifiedAt("2020-02-03T09:25:57.698128Z").build(),
                false
        );
        var v2 = CommunityTopStatementAdder.addStatement(v1,
                b.setProjectCount(1).setStatementId(1).setStatement(s).setAvgOrdNumOfDomain(null).setModifiedAt("2020-02-03T09:25:57.698128Z").build(),
                false
        );
        var v3 = CommunityTopStatementAdder.addStatement(v2,
                b.setProjectCount(1).setStatementId(2).setStatement(s).setAvgOrdNumOfDomain(null).setModifiedAt("2020-02-03T09:25:57.698128Z").build(),
                false
        );


        assertThat(v3.size()).isEqualTo(3);
        assertThat(v3.get(0).getStatementId()).isEqualTo(2);
        assertThat(v3.get(1).getStatementId()).isEqualTo(1);
        assertThat(v3.get(2).getStatementId()).isEqualTo(0);
    }

    @Test
    void testOutgoingMoveStatementDown() {
        var s = StatementEnrichedValue.newBuilder()
                .setSubjectId("1")
                .setPropertyId(2)
                .setObjectId("3")
                .build();

        var b = CommunityStatementValue.newBuilder();
        var v0 = new ArrayList<CommunityStatementValue>();
        var v1 = CommunityTopStatementAdder.addStatement(v0,
                b.setProjectCount(1).setStatementId(1).setStatement(s).setAvgOrdNumOfRange(2f).build(), true
        );
        var v2 = CommunityTopStatementAdder.addStatement(v1,
                b.setProjectCount(1).setStatementId(2).setStatement(s).setAvgOrdNumOfRange(1f).build(), true
        );

        assertThat(v2.size()).isEqualTo(2);
        assertThat(v2.get(0).getStatementId()).isEqualTo(2);
        assertThat(v2.get(1).getStatementId()).isEqualTo(1);

        var v3 = CommunityTopStatementAdder.addStatement(v2,
                b.setProjectCount(1).setStatementId(2).setStatement(s).setAvgOrdNumOfRange(3f).build(), true
        );

        assertThat(v3.size()).isEqualTo(2);
        assertThat(v3.get(0).getStatementId()).isEqualTo(1);
        assertThat(v3.get(1).getStatementId()).isEqualTo(2);

    }

    @Test
    void testOutgoingMoveStatementUp() {
        var s = StatementEnrichedValue.newBuilder()
                .setSubjectId("1")
                .setPropertyId(2)
                .setObjectId("3")
                .build();

        var b = CommunityStatementValue.newBuilder();
        var v0 = new ArrayList<CommunityStatementValue>();
        var v1 = CommunityTopStatementAdder.addStatement(v0,
                b.setProjectCount(1).setStatementId(1).setStatement(s).setAvgOrdNumOfRange(2f).build(), true
        );
        var v2 = CommunityTopStatementAdder.addStatement(v1,
                b.setProjectCount(1).setStatementId(2).setStatement(s).setAvgOrdNumOfRange(3f).build(), true
        );

        assertThat(v2.size()).isEqualTo(2);
        assertThat(v2.get(0).getStatementId()).isEqualTo(1);
        assertThat(v2.get(1).getStatementId()).isEqualTo(2);

        var v3 = CommunityTopStatementAdder.addStatement(v2,
                b.setProjectCount(1).setStatementId(2).setStatement(s).setAvgOrdNumOfRange(1f).build(), true
        );

        assertThat(v3.size()).isEqualTo(2);
        assertThat(v3.get(0).getStatementId()).isEqualTo(2);
        assertThat(v3.get(1).getStatementId()).isEqualTo(1);

    }

    @Test
    void testOutgoingDeleteStatementUp() {
        var s = StatementEnrichedValue.newBuilder()
                .setSubjectId("1")
                .setPropertyId(2)
                .setObjectId("3")
                .build();

        var b = CommunityStatementValue.newBuilder();
        var v0 = new ArrayList<CommunityStatementValue>();
        var v1 = CommunityTopStatementAdder.addStatement(v0,
                b.setProjectCount(1).setStatementId(1).setStatement(s).setAvgOrdNumOfRange(1f).build(), true
        );
        var v2 = CommunityTopStatementAdder.addStatement(v1,
                b.setProjectCount(1).setStatementId(2).setStatement(s).setAvgOrdNumOfRange(2f).build(), true
        );
        var v3 = CommunityTopStatementAdder.addStatement(v2,
                b.setProjectCount(1).setStatementId(3).setStatement(s).setAvgOrdNumOfRange(3f).build(), true
        );
        assertThat(v3.get(0).getStatementId()).isEqualTo(1);

        var v4 = CommunityTopStatementAdder.addStatement(v3,
                b.setProjectCount(1).setStatementId(1).setStatement(s).setAvgOrdNumOfRange(3f).setProjectCount(0).build(), true
        );

        assertThat(v4.size()).isEqualTo(2);
        assertThat(v4.get(0).getStatementId()).isEqualTo(2);
    }


}
