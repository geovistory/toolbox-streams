package org.geovistory.toolbox.streams.entity.label2;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.entity.label2.docs.AsciiToMermaid;
import org.geovistory.toolbox.streams.entity.label2.lib.ConfiguredAvroSerde;
import org.geovistory.toolbox.streams.entity.label2.lib.FileWriter;
import org.geovistory.toolbox.streams.entity.label2.lib.TopicsCreator;
import org.geovistory.toolbox.streams.entity.label2.names.InputTopicNames;
import org.geovistory.toolbox.streams.entity.label2.names.OutputTopicNames;
import org.geovistory.toolbox.streams.entity.label2.partitioner.CustomPartitioner;
import org.geovistory.toolbox.streams.entity.label2.processors.*;
import org.geovistory.toolbox.streams.entity.label2.stores.*;

import static org.geovistory.toolbox.streams.entity.label2.lib.Fn.createProjectEntityKeyOfSource;
import static org.geovistory.toolbox.streams.entity.label2.names.ProcessorNames.*;
import static org.geovistory.toolbox.streams.entity.label2.names.SinkNames.*;
import static org.geovistory.toolbox.streams.entity.label2.names.SourceNames.*;

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

    @Produces
    public Topology buildTopology() {
        var topology = createTopology();
        createTopologyDocumentation(topology);
        // create output topics in advance to ensure correct configuration (partition, compaction, ect.)
        topicsCreator.createOutputTopics();
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
                .addProcessor(REPARTITION_IPR_BY_FKE,
                        RepartitionIprByFkEntity::new,
                        inputTopicNames.proInfProjRel() + "-source")
                // publish re-keyed event to a repartition topic
                .addSink(REPARTITIONED_IPR_BY_FKE_SINK,
                        outputTopicNames.iprRepartitioned(),
                        Serdes.Integer().serializer(), as.<ts.projects.info_proj_rel.Value>value().serializer(),
                        REPARTITION_IPR_BY_FKE)
                // read from repartition topic
                .addSource(REPARTITIONED_IPR_BY_FKE_SOURCE,
                        Serdes.Integer().deserializer(), as.<ts.projects.info_proj_rel.Value>value().deserializer(),
                        outputTopicNames.iprRepartitioned())

                // -----------------------------------------
                // Repartition entity (inf resource) rel by pk_entity
                // -----------------------------------------

                // add node to re-key the original event
                .addProcessor(REPARTITION_E_BY_PK,
                        RepartitionEByPk::new,
                        inputTopicNames.infResource() + "-source")
                // publish re-keyed event to a repartition topic
                .addSink(REPARTITIONED_E_BY_PK_SINK,
                        outputTopicNames.eRepartitioned(),
                        Serdes.Integer().serializer(), as.<ts.projects.info_proj_rel.Value>value().serializer(),
                        REPARTITION_E_BY_PK)
                // read from repartition topic
                .addSource(REPARTITIONED_E_BY_PK_SOURCE,
                        Serdes.Integer().deserializer(), as.<ts.projects.info_proj_rel.Value>value().deserializer(),
                        outputTopicNames.eRepartitioned())

                // -----------------------------------------
                // Repartition statement by pk_entity
                // -----------------------------------------

                // add node to re-key the original event
                .addProcessor(REPARTITION_S_BY_PK,
                        RepartitionSByPk::new,
                        inputTopicNames.getStatementWithLiteral() + "-source")
                // publish re-keyed event to a repartition topic
                .addSink(REPARTITIONED_S_BY_PK_SINK,
                        outputTopicNames.sRepartitioned(),
                        Serdes.Integer().serializer(), as.<StatementEnrichedValue>value().serializer(),
                        REPARTITION_S_BY_PK)
                // read from repartition topic
                .addSource(REPARTITIONED_S_BY_PK_SOURCE,
                        Serdes.Integer().deserializer(), as.<StatementEnrichedValue>value().deserializer(),
                        outputTopicNames.sRepartitioned())

                // -----------------------------------------
                // Join ipr with entity and statements
                // -----------------------------------------

                .addProcessor(JOIN_IPR, JoinIPR_E::new, REPARTITIONED_IPR_BY_FKE_SOURCE)
                .addProcessor(JOIN_E, JoinE_IPR::new, REPARTITIONED_E_BY_PK_SOURCE)
                .addProcessor(IPR_TO_E, IPRtoE::new, JOIN_IPR)
                .addProcessor(JOIN_S, JoinS_IPR::new, REPARTITIONED_S_BY_PK_SOURCE)
                .addProcessor(IPR_TO_S, IPRtoS::new, JOIN_IPR)


                .addSink(
                        PROJECT_ENTITY_SINK,
                        outputTopicNames.projectEntity(),
                        as.<ProjectEntityKey>key().serializer(), as.<EntityValue>value().serializer(),
                        JOIN_E, IPR_TO_E
                )
                .addSource(
                        PROJECT_ENTITY_SOURCE,
                        as.<ProjectEntityKey>key().deserializer(), as.<EntityValue>value().deserializer(),
                        outputTopicNames.projectEntity()
                )

                // ---------------------------------------------------
                // Join project statement subject with project entity
                // ---------------------------------------------------

                // repartition project statements by subject
                .addProcessor(REPARTITION_S_BY_SUBJECT, RepartitionSBySub::new, JOIN_S, IPR_TO_S)
                .addSink(
                        PROJECT_STATEMENT_REPARTITIONED_BY_SUB_SINK,
                        outputTopicNames.projectStatementBySub(),
                        as.<ProjectEntityKey>key().serializer(), as.<StatementValue>value().serializer(),
                        REPARTITION_S_BY_SUBJECT
                )
                .addSource(
                        PROJECT_STATEMENT_REPARTITIONED_BY_SUB_SOURCE,
                        as.<ProjectEntityKey>key().deserializer(), as.<StatementValue>value().deserializer(),
                        outputTopicNames.projectStatementBySub()
                )

                // join project statements with their subject project entity
                .addProcessor(JOIN_S_SUB_PE, JoinSSub_PE::new, PROJECT_STATEMENT_REPARTITIONED_BY_SUB_SOURCE)
                // stock project entity
                .addProcessor(STOCK_PE, StockPE::new, PROJECT_ENTITY_SOURCE)
                // join project entity being subject with project statements
                .addProcessor(JOIN_PE_S_SUB, JoinPE_SSub::new, STOCK_PE)

                // ---------------------------------------------------
                // Create Edges from statements with literals
                // ---------------------------------------------------
                .addProcessor(CREATE_LITERAL_EDGES, CreateLiteralEdges::new, JOIN_S_SUB_PE, JOIN_PE_S_SUB)
                .addSink(
                        PROJECT_EDGE_LITERALS_SINK,
                        outputTopicNames.projectEdges(),
                        Serdes.String().serializer(), as.<EdgeValue>value().serializer(),
                        new CustomPartitioner<>(as, (kv) -> createProjectEntityKeyOfSource(kv.value)),
                        CREATE_LITERAL_EDGES
                )

                // ---------------------------------------------------
                // Join projects statement object with project entity
                // ---------------------------------------------------
                .addProcessor(REPARTITION_S_BY_OBJECT, RepartitionSByOb::new, JOIN_S, IPR_TO_S)
                .addSink(
                        PROJECT_STATEMENT_REPARTITIONED_BY_OB_SINK,
                        outputTopicNames.projectStatementByOb(),
                        as.<ProjectEntityKey>key().serializer(), Serdes.Integer().serializer(),
                        REPARTITION_S_BY_OBJECT
                )
                .addSource(
                        PROJECT_STATEMENT_REPARTITIONED_BY_OB_SOURCE,
                        as.<ProjectEntityKey>key().deserializer(), Serdes.Integer().deserializer(),
                        outputTopicNames.projectStatementByOb()
                )
                // join project statements with their object project entity
                .addProcessor(JOIN_S_OB_PE, JoinSOb_PE::new, PROJECT_STATEMENT_REPARTITIONED_BY_OB_SOURCE)
                // join project entity being object with project statements
                .addProcessor(JOIN_PE_S_OB, JoinPE_SOb::new, STOCK_PE)
                .addSink(PROJECT_S_OB_BY_PK_SINK,
                        outputTopicNames.projectStatementWithObByPk(),
                        as.<ProjectStatementKey>key().serializer(), as.<EntityValue>value().serializer(),
                        JOIN_S_OB_PE, JOIN_PE_S_OB
                )
                .addSource(
                        PROJECT_S_OB_BY_PK_SOURCE,
                        as.<ProjectStatementKey>key().deserializer(), as.<StatementWithObValue>value().deserializer(),
                        outputTopicNames.projectStatementWithObByPk()
                )

                // ---------------------------------------------------
                // Join
                // projects statement with subject and
                // projects statement with object
                // ---------------------------------------------------
                .addProcessor(REPARTITION_PS_SUB_BY_PK, RepartitionSSubByPk::new, JOIN_S_SUB_PE, JOIN_PE_S_SUB)
                .addSink(PROJECT_S_SUB_BY_PK_SINK,
                        outputTopicNames.projectStatementWithSubByPk(),
                        as.<ProjectStatementKey>key().serializer(), as.<StatementWithSubValue>value().serializer(),
                        REPARTITION_PS_SUB_BY_PK
                )
                .addSource(
                        PROJECT_S_SUB_BY_PK_SOURCE,
                        as.<ProjectStatementKey>key().deserializer(), as.<StatementWithSubValue>value().deserializer(),
                        outputTopicNames.projectStatementWithSubByPk()
                )
                // join project statements with subject with their object project entity
                .addProcessor(JOIN_SUB_WITH_OB, JoinSub_Ob::new, PROJECT_S_SUB_BY_PK_SOURCE)
                // join object project entity with the project statements with subject
                .addProcessor(JOIN_OB_WITH_SUB, JoinOb_Sub::new, PROJECT_S_OB_BY_PK_SOURCE)

                // ---------------------------------------------------
                // Create Edges from statements with entities
                // Remark: The keys are unique per edge but
                //         the edges are partitioned by ProjectEntityKey
                //         created from sourceId and projectId
                //         (see edgePartitioner)
                // ---------------------------------------------------
                .addSink(PROJECT_EDGE_ENTITIES_SINK,
                        outputTopicNames.projectEdges(),
                        Serdes.String().serializer(), as.<EdgeValue>value().serializer(),
                        new CustomPartitioner<>(as, (kv) -> createProjectEntityKeyOfSource(kv.value)),
                        JOIN_SUB_WITH_OB, JOIN_OB_WITH_SUB
                )

                // ---------------------------------------------------
                // Join project statement subject with project entity
                // ---------------------------------------------------
                .addStateStore(eStore.createPersistentKeyValueStore(), JOIN_IPR, JOIN_E)
                .addStateStore(sStore.createPersistentKeyValueStore(), JOIN_IPR, JOIN_S)
                .addStateStore(iprStore.createPersistentKeyValueStore(), JOIN_IPR, JOIN_E, JOIN_S)
                .addStateStore(peStore.createPersistentKeyValueStore(), STOCK_PE, JOIN_S_SUB_PE, JOIN_S_OB_PE)
                .addStateStore(sSubStore.createPersistentKeyValueStore(), JOIN_PE_S_SUB, JOIN_S_SUB_PE)
                .addStateStore(sObStore.createPersistentKeyValueStore(), JOIN_PE_S_OB, JOIN_S_OB_PE, JOIN_OB_WITH_SUB)
                .addStateStore(sCompleteStore.createPersistentKeyValueStore(), JOIN_SUB_WITH_OB, JOIN_OB_WITH_SUB);
    }

    private static void createTopologyDocumentation(Topology topology) {
        var description = topology.describe();
        var content = "```mermaid\n" + AsciiToMermaid.toMermaid(description.toString()) + "\n```";
        FileWriter.write("topology.md", content);
    }     // Define a custom partitioner


}

