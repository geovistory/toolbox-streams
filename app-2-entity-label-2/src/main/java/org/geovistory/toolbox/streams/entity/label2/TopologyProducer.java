package org.geovistory.toolbox.streams.entity.label2;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.entity.label2.lib.ConfiguredAvroSerde;
import org.geovistory.toolbox.streams.entity.label2.lib.TopicsCreator;
import org.geovistory.toolbox.streams.entity.label2.names.InputTopicNames;
import org.geovistory.toolbox.streams.entity.label2.names.OutputTopicNames;
import org.geovistory.toolbox.streams.entity.label2.processors.*;
import org.geovistory.toolbox.streams.entity.label2.stores.EStore;
import org.geovistory.toolbox.streams.entity.label2.stores.IprStore;
import org.geovistory.toolbox.streams.entity.label2.stores.SwlStore;

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
    SwlStore swlStore;

    @Produces
    public Topology buildTopology() {

        // create output topics in advance to ensure correct configuration (partition, compaction, ect.)
        topicsCreator.createOutputTopics();

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
                        inputTopicNames.getStatementWithLiteral()
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

                .addProcessor(JOIN_E, JoinE::new, REPARTITIONED_E_BY_PK_SOURCE)
                .addProcessor(JOIN_SWL, JoinSWL::new, REPARTITIONED_S_BY_PK_SOURCE)
                .addProcessor(JOIN_IPR, JoinIPR::new, REPARTITIONED_IPR_BY_FKE_SOURCE)
                .addProcessor(IPR_TO_E, IPRtoE::new, JOIN_IPR)
                .addProcessor(IPR_TO_S, IPRtoS::new, JOIN_IPR)
                .addSink(
                        PROJECT_ENTITY_SINK,
                        outputTopicNames.projectEntity(),
                        as.<ProjectEntityKey>key().serializer(), as.<EntityValue>value().serializer(),
                        JOIN_E, IPR_TO_E
                )
                .addSink(
                        PROJECT_STATEMENT_SINK,
                        outputTopicNames.projectStatement(),
                        as.<ProjectStatementKey>key().serializer(), as.<StatementValue>value().serializer(),
                        JOIN_SWL, IPR_TO_S
                )
                .addStateStore(eStore.createPersistentKeyValueStore(), JOIN_IPR, JOIN_E)
                .addStateStore(swlStore.createPersistentKeyValueStore(), JOIN_IPR, JOIN_SWL)
                .addStateStore(iprStore.createPersistentKeyValueStore(), JOIN_IPR, JOIN_E, JOIN_SWL);
    }
}

