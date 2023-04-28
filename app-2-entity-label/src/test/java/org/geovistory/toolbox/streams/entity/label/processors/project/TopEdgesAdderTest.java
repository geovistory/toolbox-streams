package org.geovistory.toolbox.streams.entity.label.processors.project;


import org.geovistory.toolbox.streams.avro.ProjectEdgeValue;
import org.geovistory.toolbox.streams.entity.label.processsors.project.TopEdgesAdder;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import static org.assertj.core.api.Assertions.assertThat;

class TopEdgesAdderTest {


    @Test
    void testOutgoingOrdering() {

        var b = ProjectEdgeValue.newBuilder()
                .setSourceId("1")
                .setPropertyId(2)
                .setTargetId("3");

        var v0 = new ArrayList<ProjectEdgeValue>();
        var v1 = TopEdgesAdder.addEdge(v0,
                b.setProjectId(1).setStatementId(4).setOrdNum(4).build()
        );
        var v2 = TopEdgesAdder.addEdge(v1,
                b.setProjectId(1).setStatementId(3).setOrdNum(3).build()
        );
        var v3 = TopEdgesAdder.addEdge(v2,
                b.setProjectId(1).setStatementId(2).setOrdNum(2).build()
        );
        var v4 = TopEdgesAdder.addEdge(v3,
                b.setProjectId(1).setStatementId(5).setOrdNum(null).build()
        );
        var v5 = TopEdgesAdder.addEdge(v4,
                b.setProjectId(1).setStatementId(0).setOrdNum(0).build()
        );
        var v6 = TopEdgesAdder.addEdge(v5,
                b.setProjectId(1).setStatementId(1).setOrdNum(1).build()
        );

        assertThat(v6.size()).isEqualTo(5);
        assertThat(v6.get(0).getOrdNum()).isEqualTo(0);
        assertThat(v6.get(3).getOrdNum()).isEqualTo(3);
    }

    @Test
    void testOutgoingOrderingByModificationDate() {
         var b = ProjectEdgeValue.newBuilder()
                .setSourceId("1")
                .setPropertyId(2)
                .setTargetId("3");
        var v0 = new ArrayList<ProjectEdgeValue>();
        var v1 = TopEdgesAdder.addEdge(v0,
                b.setProjectId(1).setStatementId(3).setModifiedAt("2020-03-03T09:25:57.698128Z").build()
        );
        var v2 = TopEdgesAdder.addEdge(v1,
                b.setProjectId(1).setStatementId(4).setModifiedAt("2020-02-03T09:25:57.698128Z").build()
        );
        var v3 = TopEdgesAdder.addEdge(v2,
                b.setProjectId(1).setStatementId(0).setModifiedAt("2020-12-03T09:25:57.698128Z").build()
        );
        var v4 = TopEdgesAdder.addEdge(v3,
                b.setProjectId(1).setStatementId(1).setModifiedAt("2020-11-03T09:25:57.698128Z").build()
        );
        var v5 = TopEdgesAdder.addEdge(v4,
                b.setProjectId(1).setStatementId(2).setModifiedAt("2020-04-03T09:25:57.698128Z").build()
        );
        var v6 = TopEdgesAdder.addEdge(v5,
                b.setProjectId(1).setStatementId(5).setModifiedAt("2020-01-03T09:25:57.698128Z").build()
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
         var b = ProjectEdgeValue.newBuilder()
                .setSourceId("1")
                .setPropertyId(2)
                .setTargetId("3");
        var v0 = new ArrayList<ProjectEdgeValue>();
        var v1 = TopEdgesAdder.addEdge(v0,
                b.setProjectId(1).setStatementId(3).setOrdNum(null).setModifiedAt("2020-03-03T09:25:57.698128Z").build()
        );
        var v2 = TopEdgesAdder.addEdge(v1,
                b.setProjectId(1).setStatementId(4).setOrdNum(null).setModifiedAt("2020-02-03T09:25:57.698128Z").build()
        );
        var v3 = TopEdgesAdder.addEdge(v2,
                b.setProjectId(1).setStatementId(0).setOrdNum(null).setModifiedAt("2020-12-03T09:25:57.698128Z").build()
        );
        var v4 = TopEdgesAdder.addEdge(v3,
                b.setProjectId(1).setStatementId(1).setOrdNum(3).setModifiedAt("2020-11-03T09:25:57.698128Z").build()
        );
        var v5 = TopEdgesAdder.addEdge(v4,
                b.setProjectId(1).setStatementId(2).setOrdNum(null).setModifiedAt("2020-04-03T09:25:57.698128Z").build()
        );
        var v6 = TopEdgesAdder.addEdge(v5,
                b.setProjectId(1).setStatementId(5).setOrdNum(null).setModifiedAt("2020-01-03T09:25:57.698128Z").build()
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
         var b = ProjectEdgeValue.newBuilder()
                .setSourceId("1")
                .setPropertyId(2)
                .setTargetId("3");
        var v0 = new ArrayList<ProjectEdgeValue>();
        var v1 = TopEdgesAdder.addEdge(v0,
                b.setProjectId(1).setStatementId(0).setOrdNum(null).setModifiedAt(null).build()
        );
        var v2 = TopEdgesAdder.addEdge(v1,
                b.setProjectId(1).setStatementId(1).setOrdNum(null).setModifiedAt("2020-02-03T09:25:57.698128Z").build()
        );
        var v3 = TopEdgesAdder.addEdge(v2,
                b.setProjectId(1).setStatementId(2).setOrdNum(null).setModifiedAt(null).build()
        );


        assertThat(v3.size()).isEqualTo(3);
        assertThat(v3.get(0).getStatementId()).isEqualTo(1); // because it has a date
        assertThat(v3.get(1).getStatementId()).isEqualTo(2); // because id 2 > 0
        assertThat(v3.get(2).getStatementId()).isEqualTo(0);
    }

    @Test
    void testIncomingOrderingByStatementIdWithSameDate() {
         var b = ProjectEdgeValue.newBuilder()
                .setSourceId("1")
                .setPropertyId(2)
                .setTargetId("3");
        var v0 = new ArrayList<ProjectEdgeValue>();
        var v1 = TopEdgesAdder.addEdge(v0,
                b.setProjectId(1).setStatementId(0).setOrdNum(null).setModifiedAt("2020-02-03T09:25:57.698128Z").build()
        );
        var v2 = TopEdgesAdder.addEdge(v1,
                b.setProjectId(1).setStatementId(1).setOrdNum(null).setModifiedAt("2020-02-03T09:25:57.698128Z").build()
        );
        var v3 = TopEdgesAdder.addEdge(v2,
                b.setProjectId(1).setStatementId(2).setOrdNum(null).setModifiedAt("2020-02-03T09:25:57.698128Z").build()
        );


        assertThat(v3.size()).isEqualTo(3);
        assertThat(v3.get(0).getStatementId()).isEqualTo(2);
        assertThat(v3.get(1).getStatementId()).isEqualTo(1);
        assertThat(v3.get(2).getStatementId()).isEqualTo(0);
    }

    @Test
    void testOutgoingMoveStatementDown() {
         var b = ProjectEdgeValue.newBuilder()
                .setSourceId("1")
                .setPropertyId(2)
                .setTargetId("3");
        var v0 = new ArrayList<ProjectEdgeValue>();
        var v1 = TopEdgesAdder.addEdge(v0,
                b.setProjectId(1).setStatementId(1).setOrdNum(2).build()
        );
        var v2 = TopEdgesAdder.addEdge(v1,
                b.setProjectId(1).setStatementId(2).setOrdNum(1).build()
        );

        assertThat(v2.size()).isEqualTo(2);
        assertThat(v2.get(0).getStatementId()).isEqualTo(2);
        assertThat(v2.get(1).getStatementId()).isEqualTo(1);

        var v3 = TopEdgesAdder.addEdge(v2,
                b.setProjectId(1).setStatementId(2).setOrdNum(3).build()
        );

        assertThat(v3.size()).isEqualTo(2);
        assertThat(v3.get(0).getStatementId()).isEqualTo(1);
        assertThat(v3.get(1).getStatementId()).isEqualTo(2);

    }

    @Test
    void testOutgoingMoveStatementUp() {
         var b = ProjectEdgeValue.newBuilder()
                .setSourceId("1")
                .setPropertyId(2)
                .setTargetId("3");
        var v0 = new ArrayList<ProjectEdgeValue>();
        var v1 = TopEdgesAdder.addEdge(v0,
                b.setProjectId(1).setStatementId(1).setOrdNum(2).build()
        );
        var v2 = TopEdgesAdder.addEdge(v1,
                b.setProjectId(1).setStatementId(2).setOrdNum(3).build()
        );

        assertThat(v2.size()).isEqualTo(2);
        assertThat(v2.get(0).getStatementId()).isEqualTo(1);
        assertThat(v2.get(1).getStatementId()).isEqualTo(2);

        var v3 = TopEdgesAdder.addEdge(v2,
                b.setProjectId(1).setStatementId(2).setOrdNum(1).build()
        );

        assertThat(v3.size()).isEqualTo(2);
        assertThat(v3.get(0).getStatementId()).isEqualTo(2);
        assertThat(v3.get(1).getStatementId()).isEqualTo(1);

    }

    @Test
    void testOutgoingDeleteStatementUp() {
         var b = ProjectEdgeValue.newBuilder()
                .setSourceId("1")
                .setPropertyId(2)
                .setTargetId("3");
        var v0 = new ArrayList<ProjectEdgeValue>();
        var v1 = TopEdgesAdder.addEdge(v0,
                b.setProjectId(1).setStatementId(1).setOrdNum(1).build()
        );
        var v2 = TopEdgesAdder.addEdge(v1,
                b.setProjectId(1).setStatementId(2).setOrdNum(2).build()
        );
        var v3 = TopEdgesAdder.addEdge(v2,
                b.setProjectId(1).setStatementId(3).setOrdNum(3).build()
        );
        assertThat(v3.get(0).getStatementId()).isEqualTo(1);

        var v4 = TopEdgesAdder.addEdge(v3,
                b.setProjectId(1).setStatementId(1).setOrdNum(3).setDeleted(true).build()
        );

        assertThat(v4.size()).isEqualTo(2);
        assertThat(v4.get(0).getStatementId()).isEqualTo(2);
    }


}
