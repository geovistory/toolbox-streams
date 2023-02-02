package org.geovistory.toolbox.streams.utils;


import org.geovistory.toolbox.streams.avro.ProjectStatementValue;
import org.geovistory.toolbox.streams.avro.StatementEnrichedValue;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import static org.assertj.core.api.Assertions.assertThat;

class TopStatementAdderTest {


    @Test
    void testOutgoingOrdering() {
        var s = StatementEnrichedValue.newBuilder()
                .setSubjectId("1")
                .setPropertyId(2)
                .setObjectId("3")
                .build();

        var b = ProjectStatementValue.newBuilder();
        var v0 = new ArrayList<ProjectStatementValue>();
        var v1 = TopStatementAdder.addStatement(v0,
                b.setProjectId(1).setStatementId(4).setStatement(s).setOrdNumOfDomain(4).build(), true
        );
        var v2 = TopStatementAdder.addStatement(v1,
                b.setProjectId(1).setStatementId(3).setStatement(s).setOrdNumOfDomain(3).build(), true
        );
        var v3 = TopStatementAdder.addStatement(v2,
                b.setProjectId(1).setStatementId(2).setStatement(s).setOrdNumOfDomain(2).build(), true
        );
        var v4 = TopStatementAdder.addStatement(v3,
                b.setProjectId(1).setStatementId(5).setStatement(s).setOrdNumOfDomain(null).build(), true
        );
        var v5 = TopStatementAdder.addStatement(v4,
                b.setProjectId(1).setStatementId(0).setStatement(s).setOrdNumOfDomain(0).build(), true
        );
        var v6 = TopStatementAdder.addStatement(v5,
                b.setProjectId(1).setStatementId(1).setStatement(s).setOrdNumOfDomain(1).build(), true
        );

        assertThat(v6.size()).isEqualTo(5);
        assertThat(v6.get(0).getOrdNumOfDomain()).isEqualTo(0);
        assertThat(v6.get(3).getOrdNumOfDomain()).isEqualTo(3);
    }

    @Test
    void testOutgoingOrderingByModificationDate() {
        var s = StatementEnrichedValue.newBuilder()
                .setSubjectId("1")
                .setPropertyId(2)
                .setObjectId("3")
                .build();

        var b = ProjectStatementValue.newBuilder();
        var v0 = new ArrayList<ProjectStatementValue>();
        var v1 = TopStatementAdder.addStatement(v0,
                b.setProjectId(1).setStatementId(3).setStatement(s).setModifiedAt("2020-03-03T09:25:57.698128Z").build(), true
        );
        var v2 = TopStatementAdder.addStatement(v1,
                b.setProjectId(1).setStatementId(4).setStatement(s).setModifiedAt("2020-02-03T09:25:57.698128Z").build(), true
        );
        var v3 = TopStatementAdder.addStatement(v2,
                b.setProjectId(1).setStatementId(0).setStatement(s).setModifiedAt("2020-12-03T09:25:57.698128Z").build(), true
        );
        var v4 = TopStatementAdder.addStatement(v3,
                b.setProjectId(1).setStatementId(1).setStatement(s).setModifiedAt("2020-11-03T09:25:57.698128Z").build(), true
        );
        var v5 = TopStatementAdder.addStatement(v4,
                b.setProjectId(1).setStatementId(2).setStatement(s).setModifiedAt("2020-04-03T09:25:57.698128Z").build(), true
        );
        var v6 = TopStatementAdder.addStatement(v5,
                b.setProjectId(1).setStatementId(5).setStatement(s).setModifiedAt("2020-01-03T09:25:57.698128Z").build(), true
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

        var b = ProjectStatementValue.newBuilder();
        var v0 = new ArrayList<ProjectStatementValue>();
        var v1 = TopStatementAdder.addStatement(v0,
                b.setProjectId(1).setStatementId(3).setStatement(s).setOrdNumOfRange(null).setModifiedAt("2020-03-03T09:25:57.698128Z").build(),
                false
        );
        var v2 = TopStatementAdder.addStatement(v1,
                b.setProjectId(1).setStatementId(4).setStatement(s).setOrdNumOfRange(null).setModifiedAt("2020-02-03T09:25:57.698128Z").build(),
                false
        );
        var v3 = TopStatementAdder.addStatement(v2,
                b.setProjectId(1).setStatementId(0).setStatement(s).setOrdNumOfRange(null).setModifiedAt("2020-12-03T09:25:57.698128Z").build(),
                false
        );
        var v4 = TopStatementAdder.addStatement(v3,
                b.setProjectId(1).setStatementId(1).setStatement(s).setOrdNumOfRange(3).setModifiedAt("2020-11-03T09:25:57.698128Z").build(),
                false
        );
        var v5 = TopStatementAdder.addStatement(v4,
                b.setProjectId(1).setStatementId(2).setStatement(s).setOrdNumOfRange(null).setModifiedAt("2020-04-03T09:25:57.698128Z").build(),
                false
        );
        var v6 = TopStatementAdder.addStatement(v5,
                b.setProjectId(1).setStatementId(5).setStatement(s).setOrdNumOfRange(null).setModifiedAt("2020-01-03T09:25:57.698128Z").build(),
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

        var b = ProjectStatementValue.newBuilder();
        var v0 = new ArrayList<ProjectStatementValue>();
        var v1 = TopStatementAdder.addStatement(v0,
                b.setProjectId(1).setStatementId(0).setStatement(s).setOrdNumOfRange(null).setModifiedAt(null).build(),
                false
        );
        var v2 = TopStatementAdder.addStatement(v1,
                b.setProjectId(1).setStatementId(1).setStatement(s).setOrdNumOfRange(null).setModifiedAt("2020-02-03T09:25:57.698128Z").build(),
                false
        );
        var v3 = TopStatementAdder.addStatement(v2,
                b.setProjectId(1).setStatementId(2).setStatement(s).setOrdNumOfRange(null).setModifiedAt(null).build(),
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

        var b = ProjectStatementValue.newBuilder();
        var v0 = new ArrayList<ProjectStatementValue>();
        var v1 = TopStatementAdder.addStatement(v0,
                b.setProjectId(1).setStatementId(0).setStatement(s).setOrdNumOfRange(null).setModifiedAt("2020-02-03T09:25:57.698128Z").build(),
                false
        );
        var v2 = TopStatementAdder.addStatement(v1,
                b.setProjectId(1).setStatementId(1).setStatement(s).setOrdNumOfRange(null).setModifiedAt("2020-02-03T09:25:57.698128Z").build(),
                false
        );
        var v3 = TopStatementAdder.addStatement(v2,
                b.setProjectId(1).setStatementId(2).setStatement(s).setOrdNumOfRange(null).setModifiedAt("2020-02-03T09:25:57.698128Z").build(),
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

        var b = ProjectStatementValue.newBuilder();
        var v0 = new ArrayList<ProjectStatementValue>();
        var v1 = TopStatementAdder.addStatement(v0,
                b.setProjectId(1).setStatementId(1).setStatement(s).setOrdNumOfDomain(2).build(), true
        );
        var v2 = TopStatementAdder.addStatement(v1,
                b.setProjectId(1).setStatementId(2).setStatement(s).setOrdNumOfDomain(1).build(), true
        );

        assertThat(v2.size()).isEqualTo(2);
        assertThat(v2.get(0).getStatementId()).isEqualTo(2);
        assertThat(v2.get(1).getStatementId()).isEqualTo(1);

        var v3 = TopStatementAdder.addStatement(v2,
                b.setProjectId(1).setStatementId(2).setStatement(s).setOrdNumOfDomain(3).build(), true
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

        var b = ProjectStatementValue.newBuilder();
        var v0 = new ArrayList<ProjectStatementValue>();
        var v1 = TopStatementAdder.addStatement(v0,
                b.setProjectId(1).setStatementId(1).setStatement(s).setOrdNumOfDomain(2).build(), true
        );
        var v2 = TopStatementAdder.addStatement(v1,
                b.setProjectId(1).setStatementId(2).setStatement(s).setOrdNumOfDomain(3).build(), true
        );

        assertThat(v2.size()).isEqualTo(2);
        assertThat(v2.get(0).getStatementId()).isEqualTo(1);
        assertThat(v2.get(1).getStatementId()).isEqualTo(2);

        var v3 = TopStatementAdder.addStatement(v2,
                b.setProjectId(1).setStatementId(2).setStatement(s).setOrdNumOfDomain(1).build(), true
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

        var b = ProjectStatementValue.newBuilder();
        var v0 = new ArrayList<ProjectStatementValue>();
        var v1 = TopStatementAdder.addStatement(v0,
                b.setProjectId(1).setStatementId(1).setStatement(s).setOrdNumOfDomain(1).build(), true
        );
        var v2 = TopStatementAdder.addStatement(v1,
                b.setProjectId(1).setStatementId(2).setStatement(s).setOrdNumOfDomain(2).build(), true
        );
        var v3 = TopStatementAdder.addStatement(v2,
                b.setProjectId(1).setStatementId(3).setStatement(s).setOrdNumOfDomain(3).build(), true
        );
        assertThat(v3.get(0).getStatementId()).isEqualTo(1);

        var v4 = TopStatementAdder.addStatement(v3,
                b.setProjectId(1).setStatementId(1).setStatement(s).setOrdNumOfDomain(3).setDeleted$1(true).build(), true
        );

        assertThat(v4.size()).isEqualTo(2);
        assertThat(v4.get(0).getStatementId()).isEqualTo(2);
    }


}
