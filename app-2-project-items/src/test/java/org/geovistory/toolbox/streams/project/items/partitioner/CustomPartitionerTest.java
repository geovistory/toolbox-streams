package org.geovistory.toolbox.streams.project.items.partitioner;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.apache.kafka.clients.producer.internals.BuiltInPartitioner;
import org.apache.kafka.streams.processor.internals.DefaultStreamPartitioner;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.project.items.lib.ConfiguredAvroSerde;
import org.geovistory.toolbox.streams.project.items.lib.Fn;
import org.geovistory.toolbox.streams.testlib.TopologyTestDriverProfile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

@QuarkusTest
@TestProfile(TopologyTestDriverProfile.class)
public class CustomPartitionerTest {

    @Inject
    ConfiguredAvroSerde as;
    DefaultStreamPartitioner defaultPartitioner;
    CustomPartitioner customPartitioner;

    @BeforeEach
    public void setUp() {
        defaultPartitioner = new DefaultStreamPartitioner(as.key().serializer());
        customPartitioner = new CustomPartitioner<String, EdgeValue, ProjectEntityKey>(as, (kv) -> Fn.createProjectEntityKeyOfSource(kv.value));
    }

    @Test
    public void testPartitioner() {
        Object[][] pairs = {
                {1, "One"},
                {2, "Two"},
                {3, "Three"},
                {4, "Four"},
                {5, "Five"}
        };
        for (Object[] pair : pairs) {
            int pid = (int) pair[0];
            String eid = (String) pair[1];

            var k = Fn.createProjectEntityKey(pid, eid);
            var v = createEdge(pid, eid);
            var defPartition = getPartition("t", k, 4);
            var edgePartition = customPartitioner.partition("t", "any-key", v, 4);
            assertEquals(defPartition, edgePartition);
        }
    }

    private int getPartition(String topic, ProjectEntityKey k, int numPartitions) {
        final byte[] keyBytes = as.<ProjectEntityKey>key().serializer().serialize(topic, k);
        return BuiltInPartitioner.partitionForKey(keyBytes, numPartitions);
    }

    private static EdgeValue createEdge(int pid, String sourceId) {
        return EdgeValue.newBuilder()
                .setProjectId(pid)
                .setStatementId(0)
                .setProjectCount(0)
                .setOrdNum(0f)
                .setSourceId(sourceId)
                .setSourceEntity(Entity.newBuilder()
                        .setFkClass(0)
                        .setCommunityVisibilityWebsite(false)
                        .setCommunityVisibilityDataApi(false)
                        .setCommunityVisibilityToolbox(false)
                        .build())
                .setSourceProjectEntity(EntityValue.newBuilder()
                        .setProjectId(0)
                        .setEntityId("0")
                        .setClassId(0)
                        .setCommunityVisibilityToolbox(true)
                        .setCommunityVisibilityDataApi(true)
                        .setCommunityVisibilityWebsite(true)
                        .setProjectVisibilityDataApi(true)
                        .setProjectVisibilityWebsite(true)
                        .setDeleted(false)
                        .build())
                .setPropertyId(0)
                .setIsOutgoing(false)
                .setTargetId("0")
                .setTargetNode(NodeValue.newBuilder().setLabel("").setId("0").setClassId(0)
                        .setEntity(
                                Entity.newBuilder()
                                        .setFkClass(0)
                                        .setCommunityVisibilityWebsite(true)
                                        .setCommunityVisibilityDataApi(true)
                                        .setCommunityVisibilityToolbox(true)
                                        .build())
                        .build())
                .setTargetProjectEntity(EntityValue.newBuilder()
                        .setProjectId(0)
                        .setEntityId("0")
                        .setClassId(0)
                        .setCommunityVisibilityToolbox(true)
                        .setCommunityVisibilityDataApi(true)
                        .setCommunityVisibilityWebsite(true)
                        .setProjectVisibilityDataApi(true)
                        .setProjectVisibilityWebsite(true)
                        .setDeleted(false)
                        .build())
                .setModifiedAt("0")
                .setDeleted(false)
                .build();
    }
}
