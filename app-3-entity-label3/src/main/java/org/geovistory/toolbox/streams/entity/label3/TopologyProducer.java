package org.geovistory.toolbox.streams.entity.label3;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.entity.label3.docs.AsciiToMermaid;
import org.geovistory.toolbox.streams.entity.label3.lib.ConfiguredAvroSerde;
import org.geovistory.toolbox.streams.entity.label3.lib.FileWriter;
import org.geovistory.toolbox.streams.entity.label3.lib.TopicsCreator;
import org.geovistory.toolbox.streams.entity.label3.names.*;
import org.geovistory.toolbox.streams.entity.label3.partitioner.CustomPartitioner;
import org.geovistory.toolbox.streams.entity.label3.processors.*;
import org.geovistory.toolbox.streams.entity.label3.stores.*;

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
    @Inject
    LabelEdgeBySourceStore labelEdgeBySourceStore;
    @Inject
    EntityLabelStore entityLabelStore;
    @Inject
    LabelConfigTmstpStore labelConfigTmstpStore;
    @Inject
    LabelEdgeByTargetStore labelEdgeByTargetStore;


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
        var globalConfigStore = globalLabelConfigStore.createPersistentKeyValueStore().withLoggingDisabled();

        return new Topology()
                // Source Edges
                .addSource(Source.EDGE, stringD, as.vD(), inputTopicNames.getProjectEdges())
                // Repartition and project edges to LabelEdge
                .addProcessor(Processor.CREATE_LABEL_EDGES, CreateLabelEdges::new, Source.EDGE)
                // Sink LabelEdge partitioned by Target ID
                .addSink(
                        Sink.LABEL_EDGE_BY_TARGET,
                        outputTopicNames.labelEdgeByTarget(),
                        stringS, as.vS(),
                        new CustomPartitioner<String, LabelEdge, String>(Serdes.String().serializer(), (kv) -> kv.value.getTargetId()),
                        Processor.CREATE_LABEL_EDGES
                )
                // Source entity label config
                .addSource(Source.LABEL_CONFIG, as.kD(), as.vD(), inputTopicNames.proEntityLabelConfig())
                .addProcessor(Processor.TRANSFORM_LABEL_CONFIG, LabelConfigTransformer::new, Source.LABEL_CONFIG)
                .addSink(
                        Sink.LABEL_CONFIG_BY_PROJECT_CLASS_KEY,
                        outputTopicNames.labelConfigByProjectClass(),
                        as.kS(), as.vS(),
                        Processor.TRANSFORM_LABEL_CONFIG
                )
                // Create Global LabelConfig Store
                .addGlobalStore(
                        globalConfigStore,
                        Source.LABEL_CONFIG_BY_CLASS_KEY,
                        as.<ProjectClassKey>kD(),
                        as.<EntityLabelConfigTmstp>vD(),
                        outputTopicNames.labelConfigByProjectClass(),
                        Processor.UPDATE_GLOBAL_STORE_LABEL_CONFIG,
                        () -> new GlobalStoreUpdater<>(GlobalLabelConfigStore.NAME)
                )
                // Source label edges partitioned by source
                .addSource(Source.LABEL_EDGE_BY_SOURCE, stringD, as.vD(), outputTopicNames.labelEdgeBySource())
                // Stock label edges by source in kv store
                .addProcessor(
                        Processor.UPDATE_LABEL_EDGES_BY_SOURCE_STORE,
                        LabelEdgeBySourceStoreUpdater::new,
                        Source.LABEL_EDGE_BY_SOURCE
                )
                // Create entity labels on new edge
                .addProcessor(
                        Processor.CREATE_LABELS_ON_NEW_EDGE,
                        CreateEntityLabel::new,
                        Processor.UPDATE_LABEL_EDGES_BY_SOURCE_STORE)
                // Sink entity labels
                .addSink(
                        Sink.ENTITY_LABEL, outputTopicNames.entityLabels(), as.kS(), as.vS(),
                        new CustomPartitioner<ProjectEntityKey, EntityLabel, String>(Serdes.String().serializer(), (kv) -> kv.key.getEntityId()),
                        Processor.CREATE_LABELS_ON_NEW_EDGE
                )
                // ---------
                // Join
                // --------

                // Source label edges partitioned by target
                .addSource(Source.LABEL_EDGE_BY_TARGET, stringD, as.vD(), outputTopicNames.labelEdgeByTarget())
                // Join edges with labels
                .addProcessor(Processor.JOIN_ON_NEW_EDGE, EdgeWithLabelJoiner::new, Source.LABEL_EDGE_BY_TARGET)
                // Source labels
                .addSource(Source.LABEL, as.kD(), as.vD(), outputTopicNames.entityLabels())
                // Join labels with edges
                .addProcessor(Processor.JOIN_ON_NEW_LABEL, LabelWithEdgeJoiner::new, Source.LABEL)
                // Sink LabelEdge partitioned by Source ID
                .addSink(
                        Sink.LABEL_EDGE_BY_SOURCE,
                        outputTopicNames.labelEdgeBySource(),
                        stringS, as.vS(),
                        new CustomPartitioner<String, LabelEdge, String>(Serdes.String().serializer(), (kv) -> kv.value.getSourceId()),
                        Processor.CREATE_LABEL_EDGES,
                        Processor.JOIN_ON_NEW_EDGE,
                        Processor.JOIN_ON_NEW_LABEL
                )
                .addStateStore(labelEdgeBySourceStore.createPersistentKeyValueStore(),
                        Processor.UPDATE_LABEL_EDGES_BY_SOURCE_STORE,
                        Processor.CREATE_LABELS_ON_NEW_EDGE
                )
                .addStateStore(entityLabelStore.createPersistentKeyValueStore(),
                        Processor.CREATE_LABELS_ON_NEW_EDGE,
                        Processor.JOIN_ON_NEW_LABEL,
                        Processor.JOIN_ON_NEW_EDGE)
                .addStateStore(labelConfigTmstpStore.createPersistentKeyValueStore(),
                        Processor.CREATE_LABELS_ON_NEW_EDGE)
                .addStateStore(labelEdgeByTargetStore.createPersistentKeyValueStore(),
                        Processor.JOIN_ON_NEW_LABEL,
                        Processor.JOIN_ON_NEW_EDGE);

    }

    private static void createTopologyDocumentation(Topology topology) {
        var description = topology.describe();
        var content = "```mermaid\n" + AsciiToMermaid.toMermaid(description.toString()) + "\n```";
        FileWriter.write("topology.md", content);
    }     // Define a custom partitioner


}

