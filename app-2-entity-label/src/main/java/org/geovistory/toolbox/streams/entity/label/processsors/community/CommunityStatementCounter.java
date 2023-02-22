package org.geovistory.toolbox.streams.entity.label.processsors.community;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.*;

public abstract class CommunityStatementCounter implements Transformer<
        ProjectStatementKey, ProjectStatementValue,
        KeyValue<CommunityStatementKey, CommunityStatementValue>> {

    private final String stateStoreName;
    private KeyValueStore<CommunityStatementKey, DRMap> kvStore;


    /**
     * @param stateStoreName name of state store
     */
    public CommunityStatementCounter(String stateStoreName) {
        this.stateStoreName = stateStoreName;
    }

    @Override
    public void init(ProcessorContext context) {
        this.kvStore = context.getStateStore(stateStoreName);
    }

    @Override
    public KeyValue<CommunityStatementKey, CommunityStatementValue> transform(
            ProjectStatementKey key,
            ProjectStatementValue projectStatementValue
    ) {
        // if we have no value, we cant do anything
        if (projectStatementValue == null) return null;

        // create CommunityStatementKey
        var k = CommunityStatementKey.newBuilder().setStatementId(key.getStatementId()).build();

        // try to get existing count map
        var existingCountMap = kvStore.get(k);

        // take existing or initialize new count map
        var countMap =
                // if no existing count map
                existingCountMap == null ?
                        // initialize new count map
                        DRMap.newBuilder().build() :
                        // else take existing count map
                        existingCountMap;

        var map = countMap.getMap();


        // generate key
        var projectId = "" + projectStatementValue.getProjectId();
        if (isAccepted(projectStatementValue)) {

            // generate value
            var ordNums = DR.newBuilder()
                    .setD(projectStatementValue.getOrdNumOfDomain())
                    .setR(projectStatementValue.getOrdNumOfRange())
                    .build();

            // add item to map
            map.put(projectId, ordNums);
        }
        // else (if input record was deleted or it is not visible for toolbox community)
        else {
            // remove the project id from count map
            map.remove(projectId);
        }

        // put count map to store
        kvStore.put(k, countMap);

        // init vars
        int projectCount = 0, dCount = 0, rCount = 0, dSum = 0, rSum = 0;

        // loop over projects
        for (var x : map.values()) {

            // count projects
            projectCount = projectCount + 1;

            var d = x.getD();
            if (d != null) {
                // count how many projects have defined the ord num of domain
                dCount = dCount + 1;
                // summarize ord num of domain
                dSum = dSum + d;
            }

            var r = x.getR();
            if (r != null) {
                // count how many projects have defined the ord num of range
                rSum = rSum + r;
                // summarize ord num of range
                rCount = rCount + 1;
            }
        }

        // calculate average ord num of domain
        var avgOrdNumOfDomain = dCount > 0 ? (float) dSum / dCount : null;

        // calculate average ord num of range
        var avgOrdNumOfRange = rCount > 0 ? (float) rSum / rCount : null;

        // create CommunityStatementValue
        var v = CommunityStatementValue.newBuilder()
                .setStatementId(projectStatementValue.getStatementId())
                .setStatement(projectStatementValue.getStatement())
                .setProjectCount(projectCount)
                .setAvgOrdNumOfDomain(avgOrdNumOfDomain)
                .setAvgOrdNumOfRange(avgOrdNumOfRange)
                .setModifiedAt(projectStatementValue.getModifiedAt())
                .build();

        // return it (down stream)
        return KeyValue.pair(k, v);

    }

    protected abstract boolean isAccepted(ProjectStatementValue projectStatementValue);

    public void close() {

    }

}