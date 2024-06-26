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
    ComLabelCountStore comLabelCountStore;
    @Inject
    ComLabelRankStore comLabelRankStore;
    @Inject
    ComLabelLangRankStore comLabelLangRankStore;
    @Inject
    LabelEdgeBySourceStore labelEdgeBySourceStore;
    @Inject
    EntityLabelStore entityLabelStore;
    @Inject
    LabelConfigTmstpStore labelConfigTmstpStore;
    @Inject
    LabelEdgeByTargetStore labelEdgeByTargetStore;
    @Inject
    EdgeCountStore edgeCountStore;
    @Inject
    EdgeOrdNumStore edgeOrdNumStore;
    @Inject
    EdgeSumStore edgeSumStore;
    @Inject
    EdgeVisibilityStore edgeVisibilityStore;
    @Inject
    EntityPublicationStore entityPublicationStore;


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
                .addSource(Sources.EDGE, stringD, as.vD(), inputTopicNames.getProjectEdges())
                // Repartition and project edges to LabelEdge
                .addProcessor(Processors.CREATE_LABEL_EDGES, CreateLabelEdges::new, Sources.EDGE)
                // Sink LabelEdge partitioned by Target ID
                .addSink(
                        Sinks.LABEL_EDGE_BY_TARGET,
                        outputTopicNames.labelEdgeByTarget(),
                        stringS, as.vS(),
                        new CustomPartitioner<String, LabelEdge, String>(Serdes.String().serializer(), (kv) -> kv.value.getTargetId()),
                        Processors.CREATE_LABEL_EDGES
                )
                // Source entity label config
                .addSource(Sources.LABEL_CONFIG, as.kD(), as.vD(), inputTopicNames.proEntityLabelConfig())
                .addProcessor(Processors.TRANSFORM_LABEL_CONFIG, LabelConfigTransformer::new, Sources.LABEL_CONFIG)
                .addSink(
                        Sinks.LABEL_CONFIG_BY_PROJECT_CLASS_KEY,
                        outputTopicNames.labelConfigByProjectClass(),
                        as.kS(), as.vS(),
                        Processors.TRANSFORM_LABEL_CONFIG
                )
                // Create Global LabelConfig Store
                .addGlobalStore(
                        globalConfigStore,
                        Sources.LABEL_CONFIG_BY_CLASS_KEY,
                        as.<ProjectClassKey>kD(),
                        as.<EntityLabelConfigTmstp>vD(),
                        outputTopicNames.labelConfigByProjectClass(),
                        Processors.UPDATE_GLOBAL_STORE_LABEL_CONFIG,
                        () -> new GlobalStoreUpdater<>(GlobalLabelConfigStore.NAME)
                )
                // Source label edges partitioned by source
                .addSource(Sources.LABEL_EDGE_BY_SOURCE, stringD, as.vD(), outputTopicNames.labelEdgeBySource())
                // Stock label edges by source in kv store
                .addProcessor(
                        Processors.UPDATE_LABEL_EDGES_BY_SOURCE_STORE,
                        LabelEdgeBySourceStoreUpdater::new,
                        Sources.LABEL_EDGE_BY_SOURCE
                )
                // Create entity labels on new edge
                .addProcessor(
                        Processors.CREATE_ENTITY_LABELS,
                        CreateEntityLabel::new,
                        Processors.UPDATE_LABEL_EDGES_BY_SOURCE_STORE)
                // Re-key entity labels
                .addProcessor(
                        Processors.RE_KEY_ENTITY_LABELS,
                        ReKeyEntityLabels::new,
                        Processors.CREATE_ENTITY_LABELS)
                // Sink entity labels
                .addSink(
                        Sinks.ENTITY_LABEL, outputTopicNames.entityLabels(), as.kS(), as.vS(),
                        new CustomPartitioner<ProjectEntityKey, EntityLabel, String>(Serdes.String().serializer(), (kv) -> kv.key.getEntityId()),
                        Processors.RE_KEY_ENTITY_LABELS
                )

                // Output for entity labels for toolbox project rdf
                .addProcessor(
                        Processors.CREATE_LABEL_TOOLBOX_PROJECT,
                        CreateRdfOutput::new,
                        Processors.CREATE_ENTITY_LABELS)
                .addSink(
                        Sinks.ENTITY_LABEL_TOOLBOX_PROJECT,
                        outputTopicNames.entityLabelsToolboxProject(), as.kS(), as.vS(),
                        Processors.CREATE_LABEL_TOOLBOX_PROJECT
                )

                // Output for entity labels for toolbox community rdf
                .addProcessor(
                        Processors.CREATE_LABEL_TOOLBOX_COMMUNITY,
                        CreateRdfOutput::new,
                        Processors.CREATE_ENTITY_LABELS)
                .addSink(
                        Sinks.ENTITY_LABEL_TOOLBOX_COMMUNITY,
                        outputTopicNames.entityLabelsToolboxCommunity(), as.kS(), as.vS(),
                        Processors.CREATE_LABEL_TOOLBOX_COMMUNITY
                )

                // Output for entity labels for public project rdf
                .addProcessor(
                        Processors.CREATE_LABEL_PUBLIC_PROJECT,
                        CreateRdfOutput::new,
                        Processors.CREATE_ENTITY_LABELS)
                .addSink(
                        Sinks.ENTITY_LABEL_PUBLIC_PROJECT,
                        outputTopicNames.entityLabelsPublicProject(), as.kS(), as.vS(),
                        Processors.CREATE_LABEL_PUBLIC_PROJECT
                )

                // Output for entity labels for public community rdf
                .addProcessor(
                        Processors.CREATE_LABEL_PUBLIC_COMMUNITY,
                        CreateRdfOutput::new,
                        Processors.CREATE_ENTITY_LABELS)
                .addSink(
                        Sinks.ENTITY_LABEL_PUBLIC_COMMUNITY,
                        outputTopicNames.entityLabelsPublicCommunity(), as.kS(), as.vS(),
                        Processors.CREATE_LABEL_PUBLIC_COMMUNITY
                )


                // Output for entity language labels for public community rdf
                .addProcessor(
                        Processors.CREATE_LANG_LABEL_PUBLIC_COMMUNITY,
                        CreateRdfOutput::new,
                        Processors.CREATE_ENTITY_LABELS)
                .addSink(
                        Sinks.ENTITY_LANG_LABEL_PUBLIC_COMMUNITY,
                        outputTopicNames.entityLanguageLabelsPublicCommunity(), as.kS(), as.vS(),
                        Processors.CREATE_LANG_LABEL_PUBLIC_COMMUNITY
                )


                // Output for entity language labels for public community rdf
                .addProcessor(
                        Processors.CREATE_LANG_LABEL_TOOLBOX_COMMUNITY,
                        CreateRdfOutput::new,
                        Processors.CREATE_ENTITY_LABELS)
                .addSink(
                        Sinks.ENTITY_LANG_LABEL_TOOLBOX_COMMUNITY,
                        outputTopicNames.entityLanguageLabelsToolboxCommunity(), as.kS(), as.vS(),
                        Processors.CREATE_LANG_LABEL_TOOLBOX_COMMUNITY
                )

                // ---------
                // Join
                // --------

                // Source label edges partitioned by target
                .addSource(Sources.LABEL_EDGE_BY_TARGET, stringD, as.vD(), outputTopicNames.labelEdgeByTarget())
                // Join edges with labels
                .addProcessor(Processors.JOIN_ON_NEW_EDGE, EdgeWithLabelJoiner::new, Sources.LABEL_EDGE_BY_TARGET)
                // Source labels
                .addSource(Sources.LABEL, as.kD(), as.vD(), outputTopicNames.entityLabels())
                // Join labels with edges
                .addProcessor(Processors.JOIN_ON_NEW_LABEL, LabelWithEdgeJoiner::new, Sources.LABEL)
                // Sink LabelEdge partitioned by Source ID
                .addSink(
                        Sinks.LABEL_EDGE_BY_SOURCE,
                        outputTopicNames.labelEdgeBySource(),
                        stringS, as.vS(),
                        new CustomPartitioner<String, LabelEdge, String>(Serdes.String().serializer(), (kv) -> kv.value.getSourceId()),
                        Processors.CREATE_LABEL_EDGES,
                        Processors.JOIN_ON_NEW_EDGE,
                        Processors.JOIN_ON_NEW_LABEL
                )
                // Create community toolbox label edges
                .addProcessor(Processors.CREATE_COMMUNITY_TOOLBOX_EDGES, CreateCommunityToolboxEdges::new, Sources.LABEL_EDGE_BY_SOURCE)
                .addSink(
                        Sinks.EDGES_TOOLBOX_COMMUNITY,
                        outputTopicNames.labelEdgesToolboxCommunityBySource(),
                        stringS, as.vS(),
                        new CustomPartitioner<String, LabelEdge, String>(Serdes.String().serializer(), (kv) -> kv.value.getSourceId()),
                        Processors.CREATE_COMMUNITY_TOOLBOX_EDGES
                )
                .addStateStore(labelEdgeBySourceStore.createPersistentKeyValueStore(),
                        Processors.UPDATE_LABEL_EDGES_BY_SOURCE_STORE,
                        Processors.CREATE_ENTITY_LABELS
                )
                .addStateStore(entityLabelStore.createPersistentKeyValueStore(),
                        Processors.CREATE_ENTITY_LABELS,
                        Processors.JOIN_ON_NEW_LABEL,
                        Processors.JOIN_ON_NEW_EDGE)
                .addStateStore(labelConfigTmstpStore.createPersistentKeyValueStore(),
                        Processors.CREATE_ENTITY_LABELS)
                .addStateStore(labelEdgeByTargetStore.createPersistentKeyValueStore(),
                        Processors.JOIN_ON_NEW_LABEL,
                        Processors.JOIN_ON_NEW_EDGE)
                .addStateStore(comLabelCountStore.createPersistentKeyValueStore(),
                        Processors.CREATE_ENTITY_LABELS)
                .addStateStore(comLabelRankStore.createPersistentKeyValueStore(),
                        Processors.CREATE_ENTITY_LABELS)
                .addStateStore(comLabelLangRankStore.createPersistentKeyValueStore(),
                        Processors.CREATE_ENTITY_LABELS)
                .addStateStore(edgeCountStore.createPersistentKeyValueStore(),
                        Processors.CREATE_COMMUNITY_TOOLBOX_EDGES)
                .addStateStore(edgeOrdNumStore.createPersistentKeyValueStore(),
                        Processors.CREATE_COMMUNITY_TOOLBOX_EDGES)
                .addStateStore(edgeSumStore.createPersistentKeyValueStore(),
                        Processors.CREATE_COMMUNITY_TOOLBOX_EDGES)
                .addStateStore(edgeVisibilityStore.createPersistentKeyValueStore(),
                        Processors.CREATE_COMMUNITY_TOOLBOX_EDGES)
                .addStateStore(entityPublicationStore.createPersistentKeyValueStore(),
                        Processors.CREATE_ENTITY_LABELS);

    }

    private static void createTopologyDocumentation(Topology topology) {
        var description = topology.describe();
        var content = "```mermaid\n" + AsciiToMermaid.toMermaid(description.toString()) + "\n```";
        FileWriter.write("topology.md", content);
    }     // Define a custom partitioner


}

