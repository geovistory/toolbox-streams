package org.geovistory.toolbox.streams.entity.label3;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.avro.LabelEdge;
import org.geovistory.toolbox.streams.entity.label3.docs.AsciiToMermaid;
import org.geovistory.toolbox.streams.entity.label3.lib.ConfiguredAvroSerde;
import org.geovistory.toolbox.streams.entity.label3.lib.FileWriter;
import org.geovistory.toolbox.streams.entity.label3.lib.TopicsCreator;
import org.geovistory.toolbox.streams.entity.label3.names.*;
import org.geovistory.toolbox.streams.entity.label3.partitioner.CustomPartitioner;
import org.geovistory.toolbox.streams.entity.label3.processors.CreateLabelEdges;
import org.geovistory.toolbox.streams.entity.label3.processors.GlobalStoreUpdater;
import org.geovistory.toolbox.streams.entity.label3.stores.GlobalLabelConfigStore;

import java.util.Objects;

@ApplicationScoped
public class TopologyProducer {
    @Inject
    InputTopicNames inputTopicNames;
    @Inject
    OutputTopicNames outputTopicNames;
    @Inject
    ConfiguredAvroSerde as;
    @Inject
    TopicsCreator topicsCreator;
    @ConfigProperty(name = "auto.create.output.topics")
    String autoCreateOutputTopics;
    @Inject
    GlobalLabelConfigStore globalLabelConfigStore;

    @Produces
    public Topology buildTopology() {
        var topology = createTopology();
        createTopologyDocumentation(topology);
        // create output topics in advance to ensure correct configuration (partition, compaction, ect.)
        if (Objects.equals(autoCreateOutputTopics, "enabled")) topicsCreator.createOutputTopics();
        return topology;

    }

    private Topology createTopology() {
        var stringS = Serdes.String().serializer();
        var stringD = Serdes.String().deserializer();

        var topology = new Topology()
                // Source Edges
                .addSource(Source.EDGE, stringD, as.vD(), inputTopicNames.getProjectEdges())
                // Repartition and project edges to LabelEdge
                .addProcessor(Processor.CREATE_LABEL_EDGES, CreateLabelEdges::new, Source.EDGE)
                // Sink LabelEdge partitioned by Source ID
                .addSink(
                        Sink.LABEL_EDGE_BY_SOURCE,
                        outputTopicNames.labelEdgeBySource(),
                        stringS, as.vS(),
                        new CustomPartitioner<String, LabelEdge, String>(Serdes.String().serializer(), (kv) -> kv.value.getSourceId()),
                        Processor.CREATE_LABEL_EDGES
                )
                // Sink LabelEdge partitioned by Target ID
                .addSink(
                        Sink.LABEL_EDGE_BY_TARGET,
                        outputTopicNames.labelEdgeByTarget(),
                        stringS, as.vS(),
                        new CustomPartitioner<String, LabelEdge, String>(Serdes.String().serializer(), (kv) -> kv.value.getTargetId()),
                        Processor.CREATE_LABEL_EDGES
                )


                // Create Global LabelConfig Store
                .addGlobalStore(
                        globalLabelConfigStore.createPersistentKeyValueStore().withLoggingDisabled(),
                        Source.LABEL_EDGE,
                        as.<ts.projects.entity_label_config.Key>kD(),
                        as.<ts.projects.entity_label_config.Value>vD(),
                        inputTopicNames.proEntityLabelConfig(),
                        Processor.UPDATE_GLOBAL_STORE_LABEL_CONFIG,
                        () -> new GlobalStoreUpdater(GlobalLabelConfigStore.NAME)
                );

/*
                .addProcessor("T", LabelConfigStoreUpdater::new, Processor.UPDATE_GLOBAL_STORE_LABEL_CONFIG);
*/
        return topology;
    }

    private static void createTopologyDocumentation(Topology topology) {
        var description = topology.describe();
        var content = "```mermaid\n" + AsciiToMermaid.toMermaid(description.toString()) + "\n```";
        FileWriter.write("topology.md", content);
    }     // Define a custom partitioner


}

