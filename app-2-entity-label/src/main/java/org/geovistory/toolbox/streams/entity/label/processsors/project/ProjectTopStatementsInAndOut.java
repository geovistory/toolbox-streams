package org.geovistory.toolbox.streams.entity.label.processsors.project;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.entity.label.AvroSerdes;
import org.geovistory.toolbox.streams.entity.label.InputTopicNames;
import org.geovistory.toolbox.streams.entity.label.OutputTopicNames;
import org.geovistory.toolbox.streams.lib.JoinHelper;
import org.geovistory.toolbox.streams.lib.Utils;
import org.jboss.logging.Logger;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;


@ApplicationScoped
public class ProjectTopStatementsInAndOut {

    @Inject
    AvroSerdes avroSerdes;

    @Inject
    InputTopicNames inputTopicNames;

    @Inject
    OutputTopicNames outputTopicNames;

    static final Map<String, String> changelogConfig = new HashMap<>();

    public ProjectTopStatementsInAndOut(AvroSerdes avroSerdes, InputTopicNames inputTopicNames, OutputTopicNames outputTopicNames) {
        this.avroSerdes = avroSerdes;
        this.inputTopicNames = inputTopicNames;
        this.outputTopicNames = outputTopicNames;
    }

    public static void main(String[] args) {
        var t = new Topology();
        var a = new AvroSerdes();
        a.QUARKUS_KAFKA_STREAMS_SCHEMA_REGISTRY_URL = "http://foo.bar";
        new ProjectTopStatementsInAndOut(a, new InputTopicNames(), new OutputTopicNames())
                .addProcessors(t);
        System.out.println(t.describe());
    }

    /**
     * setup used for testing
     */
    public void standalone(Topology topology) {
        topology.addSource(
                // in future this can be replaced with P.COM_TLBX_LABEL_SOURCE.s()
                outputTopicNames.communityToolboxEntityLabel() + "-consumer",
                avroSerdes.CommunityEntityKey().deserializer(),
                avroSerdes.CommunityEntityLabelValue().deserializer(),
                outputTopicNames.communityToolboxEntityLabel()
        );
        addProcessors(topology);
    }

    public void addProcessors(Topology topology) {

        repartitionProjectEntityLabels(topology);

        repartitionCommunityEntityLabels(topology);

        repartitionStatements(topology);


        registerLabelState(topology);

        joinSubjectLabels(topology);

        joinObjectLabels(topology);

        aggregateIncomingStmts(topology);

        aggregateOutgoingStmts(topology);

        // store: incoming statement with labels store
        // Key (String): "{subjectId}-{projectId}-{statementId}", Value (String): ProjectStatementValue
        JoinHelper.addStateStore(topology, S.PRO_STMT_BY_SUBJECT_STORE.v(), P.PRO_STMT_BY_SUBJECT_PROCESSOR.s(), Serdes.String(), avroSerdes.ProjectStatementValue(), P.PRO_INC_STATEMENT_LABEL_JOINER.s());


        // store: outgoing statement with labels store
        // Key (String): "{subjectId}-{projectId}-{statementId}", Value (String): ProjectStatementValue
        JoinHelper.addStateStore(topology, S.PRO_STMT_BY_OBJECT_STORE.v(), P.PRO_STMT_BY_OBJECT_PROCESSOR.s(), Serdes.String(), avroSerdes.ProjectStatementValue(), P.PRO_OUT_STATEMENT_LABEL_JOINER.s());

        // store: projectLabelStore
        // Key (String): "{subjectId}-{projectId}", Value (String): "{label}"
        JoinHelper.addStateStore(topology, S.PRO_LABEL_STORE.v(), P.PRO_LABEL_BY_ID_PROCESSOR.s(), Serdes.String(), avroSerdes.ProjectEntityLabelValue(),
                P.PRO_INC_STATEMENT_LABEL_JOINER.s(),
                P.PRO_OUT_STATEMENT_LABEL_JOINER.s()
        );

        // store: communityLabelStore
        // Key (String): "{subjectId}", Value (String): "{label}"
        JoinHelper.addStateStore(topology, S.COM_TLBX_LABEL_STORE.v(), P.COM_TLBX_LABEL_BY_ID_PROCESSOR.s(), Serdes.String(), avroSerdes.CommunityEntityLabelValue(),
                P.PRO_INC_STATEMENT_LABEL_JOINER.s(),
                P.PRO_OUT_STATEMENT_LABEL_JOINER.s()
        );
    }

    private static void registerLabelState(Topology topology) {
        // processor: projectLabelProcessor
        // put the messages of projectLabelTopic in projectLabelStore
        topology.addProcessor(P.PRO_LABEL_BY_ID_PROCESSOR.s(),
                () -> new JoinHelper.InputProcessorWithStoreKey<String, ProjectEntityLabelValue, String>(
                        S.PRO_LABEL_STORE.v(),
                        (record) -> {
                            var v = record.value();
                            return v.getEntityId() + "-" + v.getProjectId();
                        }),
                P.PRO_LABEL_BY_ID_SOURCE.s()
        );


        // processor: communityLabelProcessor
        // put the messages of communityLabelTopic in communityLabelStore
        topology.addProcessor(P.COM_TLBX_LABEL_BY_ID_PROCESSOR.s(),
                () -> new JoinHelper.InputProcessorWithStoreKey<String, CommunityEntityLabelValue, String>(
                        S.COM_TLBX_LABEL_STORE.v(),
                        (record) -> {
                            var v = record.value();
                            return v.getEntityId();
                        }),
                P.COM_TLBX_LABEL_BY_ID_SOURCE.s()
        );
    }

    private void aggregateIncomingStmts(Topology topology) {
        topology.addSource(P.PRO_INC_STATEMENT_LABEL_SOURCE.s(),
                avroSerdes.ProjectStatementKey().deserializer(),
                avroSerdes.ProjectEntityLabelValue().deserializer(),
                outputTopicNames.projectIncomingStatements()
        );

        StoreBuilder<KeyValueStore<ProjectStatementKey, ProjectStatementValue>> aggregateStoreSupplier =
                Stores.keyValueStoreBuilder(
                                Stores.persistentKeyValueStore(S.PRO_TOP_INC_STMT_STORE.v()),
                                avroSerdes.ProjectStatementKey(),
                                avroSerdes.ProjectStatementValue())
                        .withLoggingEnabled(changelogConfig);


        topology.addProcessor(
                        P.PRO_TOP_INC_STATEMENT_AGGREGATOR.s(),
                        () -> new Aggregator(false),
                        P.PRO_INC_STATEMENT_LABEL_SOURCE.s())
                .addStateStore(aggregateStoreSupplier, P.PRO_TOP_INC_STATEMENT_AGGREGATOR.s())

                .addSink(P.PRO_TOP_INC_STATEMENT_SINK.s(),
                        outputTopicNames.projectTopIncomingStatements(),
                        avroSerdes.ProjectTopStatementsKey().serializer(),
                        avroSerdes.ProjectTopStatementsValue().serializer(),
                        P.PRO_TOP_INC_STATEMENT_AGGREGATOR.s()
                );
    }

    private void aggregateOutgoingStmts(Topology topology) {
        topology.addSource(P.PRO_OUT_STATEMENT_LABEL_SOURCE.s(),
                avroSerdes.ProjectStatementKey().deserializer(),
                avroSerdes.ProjectEntityLabelValue().deserializer(),
                outputTopicNames.projectOutgoingStatements(),
                outputTopicNames.projectStatementWithLiteral()
        );

        StoreBuilder<KeyValueStore<ProjectStatementKey, ProjectStatementValue>> aggregateStoreSupplier =
                Stores.keyValueStoreBuilder(
                                Stores.persistentKeyValueStore(S.PRO_TOP_OUT_STMT_STORE.v()),
                                avroSerdes.ProjectStatementKey(),
                                avroSerdes.ProjectStatementValue())
                        .withLoggingEnabled(changelogConfig);


        topology.addProcessor(
                        P.PRO_TOP_OUT_STATEMENT_AGGREGATOR.s(),
                        () -> new Aggregator(true),
                        P.PRO_OUT_STATEMENT_LABEL_SOURCE.s()
                )
                .addStateStore(aggregateStoreSupplier, P.PRO_TOP_OUT_STATEMENT_AGGREGATOR.s())

                .addSink(P.PRO_TOP_OUT_STATEMENT_SINK.s(),
                        outputTopicNames.projectTopOutgoingStatements(),
                        avroSerdes.ProjectTopStatementsKey().serializer(),
                        avroSerdes.ProjectTopStatementsValue().serializer(),
                        P.PRO_TOP_OUT_STATEMENT_AGGREGATOR.s()
                );
    }

    private void joinSubjectLabels(Topology topology) {
        // processor: statementProcessor
        // puts projectStatementWithEntityTable in statementStore
        topology.addProcessor(P.PRO_STMT_BY_SUBJECT_PROCESSOR.s(),
                () -> new JoinHelper.InputProcessorWithStoreKey<String, ProjectStatementValue, String>(
                        S.PRO_STMT_BY_SUBJECT_STORE.v(),
                        (record) -> getStatementBySubjectKey(record.value())),
                P.PRO_STMT_BY_SUBJECT_SOURCE.s()
        );


        // create a join store builder
        var joinStateStoreName = S.PRO_INC_STMT_LABEL_JOIN_STORE.v();
        StoreBuilder<KeyValueStore<ProjectStatementKey, ProjectStatementValue>> joinStoreSupplier =
                Stores.keyValueStoreBuilder(
                                Stores.persistentKeyValueStore(joinStateStoreName),
                                avroSerdes.ProjectStatementKey(),
                                avroSerdes.ProjectStatementValue())
                        .withLoggingEnabled(changelogConfig);

        // create a statement-without-project-label store builder
        StoreBuilder<KeyValueStore<String, ProjectStatementValue>> stmtWithoutProjectLabelStoreSupplier =
                Stores.keyValueStoreBuilder(
                                Stores.persistentKeyValueStore(S.PRO_INC_STMT_WITHOUT_PROJECT_LABEL_STORE.v()),
                                Serdes.String(),
                                avroSerdes.ProjectStatementValue())
                        .withLoggingEnabled(changelogConfig);


        topology.addProcessor(P.PRO_INC_STATEMENT_LABEL_JOINER.s(), () -> new JoinProcessor(true),
                        P.PRO_STMT_BY_SUBJECT_PROCESSOR.s(),
                        P.PRO_LABEL_BY_ID_PROCESSOR.s(),
                        P.COM_TLBX_LABEL_BY_ID_PROCESSOR.s())
                .addStateStore(joinStoreSupplier, P.PRO_INC_STATEMENT_LABEL_JOINER.s())
                .addStateStore(stmtWithoutProjectLabelStoreSupplier, P.PRO_INC_STATEMENT_LABEL_JOINER.s());


        topology.addSink(P.PRO_INC_STATEMENT_LABEL_SINK.s(),
                outputTopicNames.projectIncomingStatements(),
                avroSerdes.ProjectStatementKey().serializer(),
                avroSerdes.ProjectEntityLabelValue().serializer(),
                P.PRO_INC_STATEMENT_LABEL_JOINER.s()
        );
    }


    private void joinObjectLabels(Topology topology) {
        // processor: statementProcessor
        // puts projectStatementWithEntityTable in statementStore
        topology.addProcessor(P.PRO_STMT_BY_OBJECT_PROCESSOR.s(),
                () -> new JoinHelper.InputProcessorWithStoreKey<String, ProjectStatementValue, String>(
                        S.PRO_STMT_BY_OBJECT_STORE.v(),
                        (record) -> getStatementByObjectKey(record.value())),
                P.PRO_STMT_BY_OBJECT_SOURCE.s()
        );

        // create a join store builder
        var joinStateStoreName = S.PRO_OUT_STMT_LABEL_JOIN_STORE.v();
        StoreBuilder<KeyValueStore<ProjectStatementKey, ProjectStatementValue>> joinStoreSupplier =
                Stores.keyValueStoreBuilder(
                                Stores.persistentKeyValueStore(joinStateStoreName),
                                avroSerdes.ProjectStatementKey(),
                                avroSerdes.ProjectStatementValue())
                        .withLoggingEnabled(changelogConfig);

        // create a statement-without-project-label store builder
        StoreBuilder<KeyValueStore<String, ProjectStatementValue>> stmtWithoutProjectLabelStoreSupplier =
                Stores.keyValueStoreBuilder(
                                Stores.persistentKeyValueStore(S.PRO_OUT_STMT_WITHOUT_PROJECT_LABEL_STORE.v()),
                                Serdes.String(),
                                avroSerdes.ProjectStatementValue())
                        .withLoggingEnabled(changelogConfig);


        topology.addProcessor(P.PRO_OUT_STATEMENT_LABEL_JOINER.s(), () -> new JoinProcessor(false),
                        P.PRO_STMT_BY_SUBJECT_PROCESSOR.s(),
                        P.PRO_LABEL_BY_ID_PROCESSOR.s(),
                        P.COM_TLBX_LABEL_BY_ID_PROCESSOR.s())
                .addStateStore(joinStoreSupplier, P.PRO_OUT_STATEMENT_LABEL_JOINER.s())
                .addStateStore(stmtWithoutProjectLabelStoreSupplier, P.PRO_OUT_STATEMENT_LABEL_JOINER.s());


        topology.addSink(P.PRO_OUT_STATEMENT_LABEL_SINK.s(),
                outputTopicNames.projectOutgoingStatements(),
                avroSerdes.ProjectStatementKey().serializer(),
                avroSerdes.ProjectEntityLabelValue().serializer(),
                P.PRO_OUT_STATEMENT_LABEL_JOINER.s()
        );
    }

    private void repartitionStatements(Topology topology) {
        // Repartition Statements

        topology.addSource(P.PRO_STMT_SOURCE.s(),
                        avroSerdes.ProjectStatementKey().deserializer(),
                        avroSerdes.ProjectStatementValue().deserializer(),
                        outputTopicNames.projectStatementWithEntity()
                )

                // repartition by Key (String): "{subjectId}"
                .addProcessor(P.PRO_STMT_REPARTITION_BY_SUBJECT.s(),
                        () -> new RepartitionProcessor<ProjectStatementKey, ProjectStatementValue, String, ProjectStatementValue>(record -> record.withKey(record.value().getStatement().getSubjectId())),
                        P.PRO_STMT_SOURCE.s()
                )
                .addSink(P.PRO_STMT_BY_SUBJECT_SINK.s(),
                        outputTopicNames.projectStatementsBySubjectId(),
                        new StringSerializer(),
                        avroSerdes.ProjectStatementValue().serializer(),
                        P.PRO_STMT_REPARTITION_BY_SUBJECT.s()
                )
                .addSource(P.PRO_STMT_BY_SUBJECT_SOURCE.s(),
                        new StringDeserializer(),
                        avroSerdes.ProjectStatementValue().deserializer(),
                        outputTopicNames.projectStatementsBySubjectId()
                )

                // repartition by Key (String): "{objectId}"
                .addProcessor(P.PRO_STMT_REPARTITION_BY_OBJECT.s(),
                        () -> new RepartitionProcessor<ProjectStatementKey, ProjectStatementValue, String, ProjectStatementValue>(record -> record.withKey(record.value().getStatement().getObjectId())),
                        P.PRO_STMT_SOURCE.s()
                )
                .addSink(P.PRO_STMT_BY_OBJECT_SINK.s(),
                        outputTopicNames.projectStatementsByObjectId(),
                        new StringSerializer(),
                        avroSerdes.ProjectStatementValue().serializer(),
                        P.PRO_STMT_REPARTITION_BY_OBJECT.s()
                )
                .addSource(P.PRO_STMT_BY_OBJECT_SOURCE.s(),
                        new StringDeserializer(),
                        avroSerdes.ProjectStatementValue().deserializer(),
                        outputTopicNames.projectStatementsByObjectId()
                );
    }

    private void repartitionCommunityEntityLabels(Topology topology) {
        // Repartition Community Labels by Entity id

        topology.addProcessor(P.COM_TLBX_LABEL_REPARTITION_BY_ID.s(),
                        // repartition communityLabel by Key (String): "{subjectId}", Value (String): "{label}"
                        () -> new RepartitionProcessor<CommunityEntityKey, CommunityEntityLabelValue, String, CommunityEntityLabelValue>(record -> record
                                .withKey(record.key().getEntityId())
                                .withValue(record.value())
                        ),
                        // reuse consumer created by streams dsl in RegisterInnerTopic
                        // in future this can be replaced with P.COM_TLBX_LABEL_SOURCE.s()
                        outputTopicNames.communityToolboxEntityLabel() + "-consumer"
                )
                .addSink(P.COM_TLBX_LABEL_BY_ID_SINK.s(),
                        outputTopicNames.communityToolboxEntityLabelByEntityId(),
                        new StringSerializer(),
                        avroSerdes.CommunityEntityLabelValue().serializer(),
                        P.COM_TLBX_LABEL_REPARTITION_BY_ID.s()
                )
                .addSource(P.COM_TLBX_LABEL_BY_ID_SOURCE.s(),
                        new StringDeserializer(),
                        avroSerdes.CommunityEntityLabelValue().deserializer(),
                        outputTopicNames.communityToolboxEntityLabelByEntityId()
                );
    }

    private void repartitionProjectEntityLabels(Topology topology) {
        topology
                // Repartition Project Labels by Entity id

                .addSource(P.PRO_LABEL_SOURCE.s(),
                        avroSerdes.ProjectEntityKey().deserializer(),
                        avroSerdes.ProjectEntityLabelValue().deserializer(),
                        outputTopicNames.projectEntityLabel()
                )
                .addProcessor(P.PRO_LABEL_REPARTITION_BY_ID.s(),
                        // repartition projectLabel by Key (String): "{subjectId}", Value (String): "{label}"
                        () -> new RepartitionProcessor<ProjectEntityKey, ProjectEntityLabelValue, String, ProjectEntityLabelValue>(record -> record
                                .withKey(record.key().getEntityId())
                                .withValue(record.value())
                        ),
                        P.PRO_LABEL_SOURCE.s()
                )
                .addSink(P.PRO_LABEL_BY_ID_SINK.s(),
                        outputTopicNames.projectEntityLabelByEntityId(),
                        new StringSerializer(),
                        avroSerdes.ProjectEntityLabelValue().serializer(),
                        P.PRO_LABEL_REPARTITION_BY_ID.s()
                )
                .addSource(P.PRO_LABEL_BY_ID_SOURCE.s(),
                        new StringDeserializer(),
                        avroSerdes.ProjectEntityLabelValue().deserializer(),
                        outputTopicNames.projectEntityLabelByEntityId()
                );


    }


    public static class RepartitionProcessor<KIn, VIn, KOut, VOut> implements Processor<KIn, VIn, KOut, VOut> {
        private static final Logger LOG = Logger.getLogger(RepartitionProcessor.class);

        ProcessorContext<KOut, VOut> context;

        Function<Record<KIn, VIn>, Record<KOut, VOut>> map;

        public RepartitionProcessor(Function<Record<KIn, VIn>, Record<KOut, VOut>> map) {
            this.map = map;
        }

        @SuppressWarnings("unchecked")
        @Override

        public void init(final ProcessorContext context) {
            this.context = context;
        }

        @Override
        public void process(Record<KIn, VIn> record) {

            try {

                // Send event downstream--new key + original value in our case
                context.forward(map.apply(record));
            } catch (Exception e) {
                LOG.warn(e.getMessage());
            }
        }

        @Override
        public void close() {
        }
    }


    private static class JoinProcessor implements Processor<String, Object, ProjectStatementKey, ProjectStatementValue> {

        // input store
        private KeyValueStore<String, ProjectStatementValue> statementStore;


        // input store
        private KeyValueStore<String, ProjectEntityLabelValue> projectLabelStore;
        private KeyValueStore<String, CommunityEntityLabelValue> communityLabelStore;

        // inner store
        private KeyValueStore<String, ProjectStatementValue> statementWithoutProjectLabelStore;

        // output store
        private KeyValueStore<ProjectStatementKey, ProjectStatementValue> joinedKeyValueStore;

        boolean joinSubject;


        public JoinProcessor(boolean joinSubject) {
            this.joinSubject = joinSubject;
        }


        private ProcessorContext<ProjectStatementKey, ProjectStatementValue> context;


        String getEntityId(StatementEnrichedValue s) {
            return joinSubject ? s.getSubjectId() : s.getObjectId();
        }

        void setLabel(StatementEnrichedValue s, String label) {
            if (joinSubject) s.setSubjectLabel(label);
            else s.setObjectLabel(label);
        }

        String getStmtKey(ProjectStatementValue v) {
            return joinSubject ? getStatementBySubjectKey(v) : getStatementByObjectKey(v);
        }

        @Override
        public void process(Record<String, Object> record) {


            ProjectStatementKey outKey;
            ProjectStatementValue outVal;

            // join the statements with project label or community label

            // when a new statement arrives,
            if (record.value() instanceof ProjectStatementValue projectStatementValue) {
                var s = projectStatementValue.getStatement();
                var projectLabel = projectLabelStore.get(getEntityId(s));

                // if projectLabel exists: take the projectLabel
                if (projectLabel != null) {
                    setLabel(s, projectLabel.getLabel());
                }

                // else
                else {

                    var communityLabel = communityLabelStore.get(getEntityId(s));

                    // if communityLabel exists: take the communityLabel
                    if (communityLabel != null) {
                        setLabel(s, communityLabel.getLabel());
                    }

                    // put it a statementWithoutProjectLabelStore to keep track of statements to
                    // be joined when a community label arrives
                    statementWithoutProjectLabelStore.put(getStmtKey(projectStatementValue), projectStatementValue);

                }


                outKey = getKey(projectStatementValue);
                outVal = projectStatementValue;


                forwardIfDifferent(record, outKey, outVal);

                // when a new projectLabel arrives,
            } else if (record.value() instanceof ProjectEntityLabelValue projectLabelValue) {
                var projectLabel = projectLabelValue.getLabel();
                // prefixScan statementStore with {objectId/subjectId}-{projectId}

                var deleted = Utils.booleanIsEqualTrue(projectLabelValue.getDeleted$1());
                try (KeyValueIterator<String, ProjectStatementValue> iterator = statementStore.prefixScan(record.key(), new StringSerializer())) {


                    while (iterator.hasNext()) {
                        var item = iterator.next();

                        outKey = getKey(item.value);

                        // if deleted
                        if (deleted) {
                            // join community label
                            var communityLabelValue = communityLabelStore.get(projectLabelValue.getEntityId());
                            if (communityLabelValue != null) {
                                setLabel(item.value.getStatement(), communityLabelValue.getLabel());
                            }
                            // add stmt to statementWithoutProjectLabelStore
                            statementWithoutProjectLabelStore.put(getStmtKey(item.value), item.value);

                        } else {
                            // else
                            setLabel(item.value.getStatement(), projectLabel);
                        }
                        outVal = item.value;


                        // join the values and forward messages
                        forwardIfDifferent(record, outKey, outVal);

                    }
                }

                if (!deleted) {
                    // delete records prefixed with {objectId/subjectId}-{projectId} from statementWithoutProjectLabelStore
                    try (KeyValueIterator<String, ProjectStatementValue> iterator = statementWithoutProjectLabelStore.prefixScan(record.key(), new StringSerializer())) {
                        while (iterator.hasNext()) {
                            var item = iterator.next();
                            statementWithoutProjectLabelStore.delete(item.key);
                        }
                    }
                }


            }
            // when a new communityLabel arrives
            else if (record.value() instanceof CommunityEntityLabelValue communityLabelValue) {
                var communityLabel = communityLabelValue.getLabel();
                // prefixScan statementWithoutProjectLabelStore and update them and forward messages
                try (KeyValueIterator<String, ProjectStatementValue> iterator = statementWithoutProjectLabelStore.prefixScan(record.key(), new StringSerializer())) {

                    while (iterator.hasNext()) {
                        var item = iterator.next();

                        outKey = getKey(item.value);

                        if (Utils.booleanIsEqualTrue(communityLabelValue.getDeleted$1())) {
                            setLabel(item.value.getStatement(), null);
                        } else {
                            setLabel(item.value.getStatement(), communityLabel);
                        }
                        outVal = item.value;


                        // join the values and forward messages
                        forwardIfDifferent(record, outKey, outVal);

                    }
                }

            }
        }


        /**
         * Compares the old value with outKey with outVal; if different, updates state store and forwards record.
         *
         * @param record incoming record
         * @param outKey key of the downstream record
         * @param outVal value of the downstream record
         */
        private void forwardIfDifferent(Record<String, Object> record, ProjectStatementKey outKey, ProjectStatementValue outVal) {
            var oldVal = joinedKeyValueStore.get(outKey);

            if (oldVal != outVal) {
                // Put into state store.
                joinedKeyValueStore.put(outKey, outVal);

                // forward join result
                context.forward(record
                        .withKey(outKey)
                        .withValue(outVal));
            }
        }


        @Override
        public void init(ProcessorContext<ProjectStatementKey, ProjectStatementValue> context) {

            this.joinedKeyValueStore = joinSubject ?
                    context.getStateStore(S.PRO_INC_STMT_LABEL_JOIN_STORE.v()) :
                    context.getStateStore(S.PRO_OUT_STMT_LABEL_JOIN_STORE.v());

            this.statementWithoutProjectLabelStore = joinSubject ?
                    context.getStateStore(S.PRO_INC_STMT_WITHOUT_PROJECT_LABEL_STORE.v()) :
                    context.getStateStore(S.PRO_OUT_STMT_WITHOUT_PROJECT_LABEL_STORE.v());

            this.statementStore = joinSubject ?
                    context.getStateStore(S.PRO_STMT_BY_SUBJECT_STORE.v()) :
                    context.getStateStore(S.PRO_STMT_BY_OBJECT_STORE.v());

            this.projectLabelStore = context.getStateStore(S.PRO_LABEL_STORE.v());
            this.communityLabelStore = context.getStateStore(S.COM_TLBX_LABEL_STORE.v());


            this.context = context;
        }


        private ProjectStatementKey getKey(ProjectStatementValue projectStatementValue) {
            return ProjectStatementKey.newBuilder()
                    .setProjectId(projectStatementValue.getProjectId())
                    .setStatementId(projectStatementValue.getStatementId())
                    .build();
        }
    }


    private static class Aggregator implements Processor<ProjectStatementKey, ProjectStatementValue, ProjectTopStatementsKey, ProjectTopStatementsValue> {

        private KeyValueStore<ProjectTopStatementsKey, ProjectTopStatementsValue> topStatementsStore;
        private ProcessorContext<ProjectTopStatementsKey, ProjectTopStatementsValue> context;

        boolean isOutgoing;

        public Aggregator(boolean isOutgoing) {
            this.isOutgoing = isOutgoing;
        }

        String getEntityId(StatementEnrichedValue v) {
            return isOutgoing ? v.getSubjectId() : v.getObjectId();
        }

        Integer getClassId(StatementEnrichedValue v) {
            return isOutgoing ? v.getSubjectClassId() : v.getObjectClassId();

        }

        @Override
        public void process(Record<ProjectStatementKey, ProjectStatementValue> record) {
            var newValue = record.value();
            var aggKey = ProjectTopStatementsKey.newBuilder()
                    .setProjectId(newValue.getProjectId())
                    .setEntityId(getEntityId(newValue.getStatement()))
                    .setPropertyId(newValue.getStatement().getPropertyId())
                    .setIsOutgoing(isOutgoing)
                    .build();

            var aggValue = topStatementsStore.get(aggKey);
            if (aggValue == null) {
                aggValue = ProjectTopStatementsValue.newBuilder()
                        .setEntityId(aggKey.getEntityId())
                        .setPropertyId(aggKey.getPropertyId())
                        .setProjectId(aggKey.getProjectId())
                        .setStatements(new ArrayList<>())
                        .setIsOutgoing(isOutgoing)
                        .build();
            }


            List<ProjectStatementValue> statements = aggValue.getStatements();
            var newStatements = TopStatementAdder.addStatement(statements, newValue, isOutgoing);
            // extract class id of entity from new statements, if there are, or from old, if there are
            var stmts = newStatements.size() > 0 ? newStatements : aggValue.getStatements();
            if (stmts.size() > 0) {
                var firstStatement = newStatements.get(0).getStatement();
                aggValue.setClassId(getClassId(firstStatement));
            }
            aggValue.setStatements(newStatements);

            topStatementsStore.put(aggKey, aggValue);

            context.forward(record.withKey(aggKey).withValue(aggValue));
        }

        @Override
        public void init(ProcessorContext<ProjectTopStatementsKey, ProjectTopStatementsValue> context) {

            this.topStatementsStore = isOutgoing ?
                    context.getStateStore(S.PRO_TOP_OUT_STMT_STORE.v()) :
                    context.getStateStore(S.PRO_TOP_INC_STMT_STORE.v());

            this.context = context;
        }

    }


    /**
     * Converts ProjectStatementValue into key (see return)
     * The "-o" suffix stands for outgoing
     *
     * @param projectStatementValue the project statement value for which to create a key
     * @return String with pattern {subjectId}-{projectId}-{statementId}-o
     */
    static String getStatementBySubjectKey(ProjectStatementValue projectStatementValue) {
        return projectStatementValue.getStatement().getSubjectId() +
                "-" + projectStatementValue.getProjectId() +
                "-" + projectStatementValue.getStatementId() + "-o";
    }

    /**
     * Converts ProjectStatementValue into key (see return)
     * The "-i" suffix stands for incoming
     *
     * @param projectStatementValue the project statement value for which to create a key
     * @return String with pattern {objectId}-{projectId}-{statementId}-i
     */
    static String getStatementByObjectKey(ProjectStatementValue projectStatementValue) {
        return projectStatementValue.getStatement().getObjectId() +
                "-" + projectStatementValue.getProjectId() +
                "-" + projectStatementValue.getStatementId();
    }


}
