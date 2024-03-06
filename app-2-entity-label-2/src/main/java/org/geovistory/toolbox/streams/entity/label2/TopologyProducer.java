package org.geovistory.toolbox.streams.entity.label2;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import org.apache.kafka.streams.Topology;
import org.geovistory.toolbox.streams.avro.ProjectEntityKey;
import org.geovistory.toolbox.streams.avro.ProjectEntityVisibilitiesValue;
import org.geovistory.toolbox.streams.entity.label2.lib.ConfiguredAvroSerde;
import org.geovistory.toolbox.streams.entity.label2.lib.TopicsCreator;
import org.geovistory.toolbox.streams.entity.label2.names.InputTopicNames;
import org.geovistory.toolbox.streams.entity.label2.names.OutputTopicNames;
import org.geovistory.toolbox.streams.entity.label2.processors.JoinE;
import org.geovistory.toolbox.streams.entity.label2.processors.JoinerIPR;
import org.geovistory.toolbox.streams.entity.label2.processors.RepartitionIprByFkEntity;
import org.geovistory.toolbox.streams.entity.label2.stores.EStore;
import org.geovistory.toolbox.streams.entity.label2.stores.IprStore;

import static org.geovistory.toolbox.streams.entity.label2.names.ProcessorNames.*;
import static org.geovistory.toolbox.streams.entity.label2.names.SinkNames.PROJECT_ENTITY_SINK;
import static org.geovistory.toolbox.streams.entity.label2.names.SinkNames.REPARTITIONED_IPR_BY_FKE_SINK;
import static org.geovistory.toolbox.streams.entity.label2.names.SourceNames.REPARTITIONED_IPR_BY_FKE_SOURCE;

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

                // -----------------------------------------
                // Repartition info project rel by fk_entity
                // -----------------------------------------

                // add node to re-key the  original event
                .addProcessor(REPARTITION_IPR_BY_FKE,
                        RepartitionIprByFkEntity::new,
                        inputTopicNames.proInfProjRel() + "-source")
                // publish re-keyed event to a repartition topic
                .addSink(REPARTITIONED_IPR_BY_FKE_SINK,
                        outputTopicNames.iprRepartitioned(),
                        as.<ts.information.resource.Key>key().serializer(), as.<ts.projects.info_proj_rel.Value>value().serializer(),
                        REPARTITION_IPR_BY_FKE)

                // read from repartition topic
                .addSource(REPARTITIONED_IPR_BY_FKE_SOURCE,
                        as.<ts.information.resource.Key>key().deserializer(), as.<ts.projects.info_proj_rel.Value>value().deserializer(),
                        outputTopicNames.iprRepartitioned())

                // -----------------------------------------
                // Join ipr with entity and statements
                // -----------------------------------------

                .addProcessor(JOIN_E, JoinE::new, inputTopicNames.infResource() + "-source")
                .addProcessor(JOIN_IPR, JoinerIPR::new, REPARTITIONED_IPR_BY_FKE_SOURCE)
                .addSink(
                        PROJECT_ENTITY_SINK,
                        outputTopicNames.projectEntity(),
                        as.<ProjectEntityKey>key().serializer(), as.<ProjectEntityVisibilitiesValue>value().serializer(),
                        JOIN_E, JOIN_IPR
                )
                .addStateStore(eStore.createPersistentKeyValueStore(), JOIN_E, JOIN_IPR)
                .addStateStore(iprStore.createPersistentKeyValueStore(), JOIN_E, JOIN_IPR);
    }
}

