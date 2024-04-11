package org.geovistory.toolbox.streams.project.items;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.project.items.docs.AsciiToMermaid;
import org.geovistory.toolbox.streams.project.items.lib.ConfiguredAvroSerde;
import org.geovistory.toolbox.streams.project.items.lib.FileWriter;
import org.geovistory.toolbox.streams.project.items.lib.Fn;
import org.geovistory.toolbox.streams.project.items.lib.TopicsCreator;
import org.geovistory.toolbox.streams.project.items.names.*;
import org.geovistory.toolbox.streams.project.items.partitioner.CustomPartitioner;
import org.geovistory.toolbox.streams.project.items.processors.*;
import org.geovistory.toolbox.streams.project.items.stores.*;

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
    @Inject
    EStore eStore;
    @Inject
    IprStore iprStore;
    @Inject
    SStore sStore;
    @Inject
    PEStore peStore;
    @Inject
    SSubStore sSubStore;
    @Inject
    SObStore sObStore;
    @Inject
    SCompleteStore sCompleteStore;
    @Inject
    EdgeVisibilityStore edgeVisibilityStore;
    @Inject
    EdgeCountStore edgeCountStore;
    @Inject
    EdgeOrdNumStore edgeOrdNumStore;
    @Inject
    EdgeSumStore edgeSumStore;
    @Inject
    EntityIntStore entityIntStore;
    @Inject
    EntityBoolStore entityBoolStore;
    @ConfigProperty(name = "auto.create.output.topics")
    String autoCreateOutputTopics;

    @Produces
    public Topology buildTopology() {
        var topology = createTopology();
        createTopologyDocumentation(topology);
        // create output topics in advance to ensure correct configuration (partition, compaction, ect.)
        if (Objects.equals(autoCreateOutputTopics, "enabled")) topicsCreator.createOutputTopics();
        return topology;

    }

    private Topology createTopology() {
        return new Topology()
                .addSource(
                        inputTopicNames.infResource() + "-source",
                        as.<ts.information.resource.Key>key().deserializer(),
                        as.<ts.information.resource.Value>value().deserializer(),
                        inputTopicNames.infResource()
                )
                .addSource(
                        inputTopicNames.proInfProjRel() + "-source",
                        as.<ts.projects.info_proj_rel.Key>key().deserializer(),
                        as.<ts.projects.info_proj_rel.Value>value().deserializer(),
                        inputTopicNames.proInfProjRel()
                )
                .addSource(
                        inputTopicNames.getStatementWithLiteral() + "-source",
                        as.<ts.information.statement.Key>key().deserializer(),
                        as.<StatementEnrichedValue>value().deserializer(),
                        inputTopicNames.getStatementWithLiteral(), inputTopicNames.getStatementWithEntity()
                )

                // -----------------------------------------
                // Repartition info project rel by fk_entity
                // -----------------------------------------

                // add node to re-key the original event
                .addProcessor(ProcessorNames.REPARTITION_IPR_BY_FKE,
                        RepartitionIprByFkEntity::new,
                        inputTopicNames.proInfProjRel() + "-source")
                // publish re-keyed event to a repartition topic
                .addSink(SinkNames.REPARTITIONED_IPR_BY_FKE_SINK,
                        outputTopicNames.iprRepartitioned(),
                        Serdes.Integer().serializer(), as.<ts.projects.info_proj_rel.Value>value().serializer(),
                        ProcessorNames.REPARTITION_IPR_BY_FKE)
                // read from repartition topic
                .addSource(SourceNames.REPARTITIONED_IPR_BY_FKE_SOURCE,
                        Serdes.Integer().deserializer(), as.<ts.projects.info_proj_rel.Value>value().deserializer(),
                        outputTopicNames.iprRepartitioned())

                // -----------------------------------------
                // Repartition entity (inf resource) by pk_entity
                // -----------------------------------------

                // add node to re-key the original event
                .addProcessor(ProcessorNames.REPARTITION_E_BY_PK,
                        RepartitionEByPk::new,
                        inputTopicNames.infResource() + "-source")
                // publish re-keyed event to a repartition topic
                .addSink(SinkNames.REPARTITIONED_E_BY_PK_SINK,
                        outputTopicNames.eRepartitioned(),
                        Serdes.Integer().serializer(), as.<ts.projects.info_proj_rel.Value>value().serializer(),
                        ProcessorNames.REPARTITION_E_BY_PK)
                // read from repartition topic
                .addSource(SourceNames.REPARTITIONED_E_BY_PK_SOURCE,
                        Serdes.Integer().deserializer(), as.<ts.projects.info_proj_rel.Value>value().deserializer(),
                        outputTopicNames.eRepartitioned())

                // -----------------------------------------
                // Repartition statement by pk_entity
                // -----------------------------------------

                // add node to re-key the original event
                .addProcessor(ProcessorNames.REPARTITION_S_BY_PK,
                        RepartitionSByPk::new,
                        inputTopicNames.getStatementWithLiteral() + "-source")
                // publish re-keyed event to a repartition topic
                .addSink(SinkNames.REPARTITIONED_S_BY_PK_SINK,
                        outputTopicNames.sRepartitioned(),
                        Serdes.Integer().serializer(), as.<StatementEnrichedValue>value().serializer(),
                        ProcessorNames.REPARTITION_S_BY_PK)
                // read from repartition topic
                .addSource(SourceNames.REPARTITIONED_S_BY_PK_SOURCE,
                        Serdes.Integer().deserializer(), as.<StatementEnrichedValue>value().deserializer(),
                        outputTopicNames.sRepartitioned())

                // -----------------------------------------
                // Join ipr with entity and statements
                // -----------------------------------------

                .addProcessor(ProcessorNames.JOIN_IPR, JoinIPR_E::new, SourceNames.REPARTITIONED_IPR_BY_FKE_SOURCE)
                .addProcessor(ProcessorNames.JOIN_E, JoinE_IPR::new, SourceNames.REPARTITIONED_E_BY_PK_SOURCE)
                .addProcessor(ProcessorNames.IPR_TO_E, IPRtoE::new, ProcessorNames.JOIN_IPR)
                .addProcessor(ProcessorNames.JOIN_S, JoinS_IPR::new, SourceNames.REPARTITIONED_S_BY_PK_SOURCE)
                .addProcessor(ProcessorNames.IPR_TO_S, IPRtoS::new, ProcessorNames.JOIN_IPR)


                .addSink(
                        SinkNames.PROJECT_ENTITY_SINK,
                        outputTopicNames.projectEntity(),
                        as.<ProjectEntityKey>key().serializer(), as.<EntityValue>value().serializer(),
                        ProcessorNames.JOIN_E, ProcessorNames.IPR_TO_E
                )
                .addProcessor(ProcessorNames.FORK_ENTITIES, ForkEntities::new, ProcessorNames.JOIN_E, ProcessorNames.IPR_TO_E)
                // publish entities for toolbox project
                .addSink(SinkNames.TOOLBOX_PROJECT_ENTITY_SINK, outputTopicNames.toolboxProjectEntities(),
                        as.kS(), as.vS(), ProcessorNames.FORK_ENTITIES)
                // publish entities for public community
                .addSink(SinkNames.TOOLBOX_COMMUNITY_ENTITY_SINK, outputTopicNames.toolboxCommunityEntities(),
                        as.kS(), as.vS(), ProcessorNames.FORK_ENTITIES)
                // publish entities for public community
                .addSink(SinkNames.PUBLIC_PROJECT_ENTITY_SINK, outputTopicNames.publicProjectEntities(),
                        as.kS(), as.vS(), ProcessorNames.FORK_ENTITIES)

                // publish entities for public community
                .addSink(SinkNames.PUBLIC_COMMUNITY_ENTITY_SINK, outputTopicNames.publicCommunityEntities(),
                        as.kS(), as.vS(), ProcessorNames.FORK_ENTITIES)

                .addSource(
                        SinkNames.PROJECT_ENTITY_SOURCE,
                        as.<ProjectEntityKey>key().deserializer(), as.<EntityValue>value().deserializer(),
                        outputTopicNames.projectEntity()
                )

                // ---------------------------------------------------
                // Join project statement subject with project entity
                // ---------------------------------------------------

                // repartition project statements by subject
                .addProcessor(ProcessorNames.REPARTITION_S_BY_SUBJECT, RepartitionSBySub::new, ProcessorNames.JOIN_S, ProcessorNames.IPR_TO_S)
                .addSink(
                        SinkNames.PROJECT_STATEMENT_REPARTITIONED_BY_SUB_SINK,
                        outputTopicNames.projectStatementBySub(),
                        as.<ProjectEntityKey>key().serializer(), as.<StatementValue>value().serializer(),
                        ProcessorNames.REPARTITION_S_BY_SUBJECT
                )
                .addSource(
                        SourceNames.PROJECT_STATEMENT_REPARTITIONED_BY_SUB_SOURCE,
                        as.<ProjectEntityKey>key().deserializer(), as.<StatementValue>value().deserializer(),
                        outputTopicNames.projectStatementBySub()
                )

                // join project statements with their subject project entity
                .addProcessor(ProcessorNames.JOIN_S_SUB_PE, JoinSSub_PE::new, SourceNames.PROJECT_STATEMENT_REPARTITIONED_BY_SUB_SOURCE)
                // stock project entity
                .addProcessor(ProcessorNames.STOCK_PE, StockPE::new, SinkNames.PROJECT_ENTITY_SOURCE)
                // join project entity being subject with project statements
                .addProcessor(ProcessorNames.JOIN_PE_S_SUB, JoinPE_SSub::new, ProcessorNames.STOCK_PE)

                // ---------------------------------------------------
                // Create Edges from statements with literals
                // ---------------------------------------------------
                .addProcessor(ProcessorNames.CREATE_LITERAL_EDGES, CreateLiteralEdges::new, ProcessorNames.JOIN_S_SUB_PE, ProcessorNames.JOIN_PE_S_SUB)

                // ---------------------------------------------------
                // Join projects statement object with project entity
                // ---------------------------------------------------
                .addProcessor(ProcessorNames.REPARTITION_S_BY_OBJECT, RepartitionSByOb::new, ProcessorNames.JOIN_S, ProcessorNames.IPR_TO_S)
                .addSink(
                        SinkNames.PROJECT_STATEMENT_REPARTITIONED_BY_OB_SINK,
                        outputTopicNames.projectStatementByOb(),
                        as.<ProjectEntityKey>key().serializer(), Serdes.Integer().serializer(),
                        ProcessorNames.REPARTITION_S_BY_OBJECT
                )
                .addSource(
                        SourceNames.PROJECT_STATEMENT_REPARTITIONED_BY_OB_SOURCE,
                        as.<ProjectEntityKey>key().deserializer(), Serdes.Integer().deserializer(),
                        outputTopicNames.projectStatementByOb()
                )
                // join project statements with their object project entity
                .addProcessor(ProcessorNames.JOIN_S_OB_PE, JoinSOb_PE::new, SourceNames.PROJECT_STATEMENT_REPARTITIONED_BY_OB_SOURCE)
                // join project entity being object with project statements
                .addProcessor(ProcessorNames.JOIN_PE_S_OB, JoinPE_SOb::new, ProcessorNames.STOCK_PE)
                .addSink(SinkNames.PROJECT_S_OB_BY_PK_SINK,
                        outputTopicNames.projectStatementWithObByPk(),
                        as.<ProjectStatementKey>key().serializer(), as.<EntityValue>value().serializer(),
                        ProcessorNames.JOIN_S_OB_PE, ProcessorNames.JOIN_PE_S_OB
                )
                .addSource(
                        SinkNames.PROJECT_S_OB_BY_PK_SOURCE,
                        as.<ProjectStatementKey>key().deserializer(), as.<StatementWithObValue>value().deserializer(),
                        outputTopicNames.projectStatementWithObByPk()
                )

                // ---------------------------------------------------
                // Join
                // projects statement with subject and
                // projects statement with object
                // ---------------------------------------------------
                .addProcessor(ProcessorNames.REPARTITION_PS_SUB_BY_PK, RepartitionSSubByPk::new, ProcessorNames.JOIN_S_SUB_PE, ProcessorNames.JOIN_PE_S_SUB)
                .addSink(SinkNames.PROJECT_S_SUB_BY_PK_SINK,
                        outputTopicNames.projectStatementWithSubByPk(),
                        as.<ProjectStatementKey>key().serializer(), as.<StatementWithSubValue>value().serializer(),
                        ProcessorNames.REPARTITION_PS_SUB_BY_PK
                )
                .addSource(
                        SinkNames.PROJECT_S_SUB_BY_PK_SOURCE,
                        as.<ProjectStatementKey>key().deserializer(), as.<StatementWithSubValue>value().deserializer(),
                        outputTopicNames.projectStatementWithSubByPk()
                )
                // join project statements with subject with their object project entity
                .addProcessor(ProcessorNames.JOIN_SUB_WITH_OB, JoinSub_Ob::new, SinkNames.PROJECT_S_SUB_BY_PK_SOURCE)
                // join object project entity with the project statements with subject
                .addProcessor(ProcessorNames.JOIN_OB_WITH_SUB, JoinOb_Sub::new, SinkNames.PROJECT_S_OB_BY_PK_SOURCE)
                // fork edges into toolbox (all) and public (some)
                .addProcessor(ProcessorNames.FORK_EDGES, ForkEdges::new,
                        ProcessorNames.JOIN_SUB_WITH_OB,
                        ProcessorNames.JOIN_OB_WITH_SUB,
                        ProcessorNames.CREATE_LITERAL_EDGES
                )
                .addProcessor(ProcessorNames.TOOLBOX_CREATE_COMMUNITY_EDGES, () -> new CreateCommunityEdges("toolbox"),
                        ProcessorNames.FORK_EDGES
                )
                .addProcessor(ProcessorNames.PUBLIC_CREATE_COMMUNITY_EDGES, () -> new CreateCommunityEdges("public"),
                        ProcessorNames.FORK_EDGES
                )

                // ---------------------------------------------------
                // Create Edges for project toolbox rdf
                // Remark: The keys are unique per edge but
                //         the edges are partitioned by ProjectEntityKey
                //         created from sourceId and projectId
                //         (see edgePartitioner)
                // ---------------------------------------------------
                .addSink(SinkNames.TOOLBOX_PROJECT_EDGE_SINK,
                        outputTopicNames.toolboxProjectEdges(),
                        Serdes.String().serializer(), as.<EdgeValue>value().serializer(),
                        new CustomPartitioner<>(as, (kv) -> Fn.createProjectEntityKeyOfSource(kv.value)),
                        ProcessorNames.FORK_EDGES
                )
                // ---------------------------------------------------
                // Create Edges for project public rdf
                // Remark: The keys are unique per edge but
                //         the edges are partitioned by ProjectEntityKey
                //         created from sourceId and projectId
                //         (see edgePartitioner)
                // ---------------------------------------------------
                .addSink(
                        SinkNames.PUBLIC_PROJECT_EDGE_SINK,
                        outputTopicNames.publicProjectEdges(),
                        Serdes.String().serializer(), as.<EdgeValue>value().serializer(),
                        new CustomPartitioner<>(as, (kv) -> Fn.createProjectEntityKeyOfSource(kv.value)),
                        ProcessorNames.FORK_EDGES
                )
                // ---------------------------------------------------
                // Create Edges for community toolbox rdf
                // Remark: The keys are unique per edge but
                //         the edges are partitioned by ProjectEntityKey
                //         created from sourceId and projectId
                //         (see edgePartitioner)
                // ---------------------------------------------------
                .addSink(
                        SinkNames.TOOLBOX_COMMUNITY_EDGE_SINK,
                        outputTopicNames.toolboxCommunityEdges(),
                        Serdes.String().serializer(), as.<EdgeValue>value().serializer(),
                        new CustomPartitioner<>(as, (kv) -> Fn.createProjectEntityKeyOfSource(kv.value)),
                        ProcessorNames.TOOLBOX_CREATE_COMMUNITY_EDGES
                )
                // ---------------------------------------------------
                // Create Edges for community public rdf
                // Remark: The keys are unique per edge but
                //         the edges are partitioned by ProjectEntityKey
                //         created from sourceId and projectId
                //         (see edgePartitioner)
                // ---------------------------------------------------
                .addSink(
                        SinkNames.PUBLIC_COMMUNITY_EDGE_SINK,
                        outputTopicNames.publicCommunityEdges(),
                        Serdes.String().serializer(), as.<EdgeValue>value().serializer(),
                        new CustomPartitioner<>(as, (kv) -> Fn.createProjectEntityKeyOfSource(kv.value)),
                        ProcessorNames.PUBLIC_CREATE_COMMUNITY_EDGES
                )

                // ---------------------------------------------------
                // Join project statement subject with project entity
                // ---------------------------------------------------
                .addStateStore(eStore.createPersistentKeyValueStore(), ProcessorNames.JOIN_IPR, ProcessorNames.JOIN_E)
                .addStateStore(sStore.createPersistentKeyValueStore(), ProcessorNames.JOIN_IPR, ProcessorNames.JOIN_S)
                .addStateStore(iprStore.createPersistentKeyValueStore(), ProcessorNames.JOIN_IPR, ProcessorNames.JOIN_E, ProcessorNames.JOIN_S)
                .addStateStore(peStore.createPersistentKeyValueStore(), ProcessorNames.STOCK_PE, ProcessorNames.JOIN_S_SUB_PE, ProcessorNames.JOIN_S_OB_PE)
                .addStateStore(sSubStore.createPersistentKeyValueStore(), ProcessorNames.JOIN_PE_S_SUB, ProcessorNames.JOIN_S_SUB_PE)
                .addStateStore(sObStore.createPersistentKeyValueStore(), ProcessorNames.JOIN_PE_S_OB, ProcessorNames.JOIN_S_OB_PE, ProcessorNames.JOIN_OB_WITH_SUB)
                .addStateStore(sCompleteStore.createPersistentKeyValueStore(), ProcessorNames.JOIN_SUB_WITH_OB, ProcessorNames.JOIN_OB_WITH_SUB)
                .addStateStore(edgeVisibilityStore.createPersistentKeyValueStore(), ProcessorNames.FORK_EDGES)
                .addStateStore(edgeOrdNumStore.createPersistentKeyValueStore(), ProcessorNames.TOOLBOX_CREATE_COMMUNITY_EDGES, ProcessorNames.PUBLIC_CREATE_COMMUNITY_EDGES)
                .addStateStore(edgeCountStore.createPersistentKeyValueStore(), ProcessorNames.TOOLBOX_CREATE_COMMUNITY_EDGES, ProcessorNames.PUBLIC_CREATE_COMMUNITY_EDGES)
                .addStateStore(edgeSumStore.createPersistentKeyValueStore(), ProcessorNames.TOOLBOX_CREATE_COMMUNITY_EDGES, ProcessorNames.PUBLIC_CREATE_COMMUNITY_EDGES)
                .addStateStore(entityIntStore.createPersistentKeyValueStore(), ProcessorNames.FORK_ENTITIES)
                .addStateStore(entityBoolStore.createPersistentKeyValueStore(), ProcessorNames.FORK_ENTITIES, ProcessorNames.FORK_EDGES);
    }

    private static void createTopologyDocumentation(Topology topology) {
        var description = topology.describe();
        var content = "```mermaid\n" + AsciiToMermaid.toMermaid(description.toString()) + "\n```";
        FileWriter.write("topology.md", content);
    }     // Define a custom partitioner


}

