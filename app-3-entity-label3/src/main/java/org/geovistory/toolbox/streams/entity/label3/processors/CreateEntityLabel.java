package org.geovistory.toolbox.streams.entity.label3.processors;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.entity.label3.names.Processors;
import org.geovistory.toolbox.streams.entity.label3.names.PubTargets;
import org.geovistory.toolbox.streams.entity.label3.stores.*;
import org.geovistory.toolbox.streams.lib.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Objects;

import static java.lang.Integer.parseInt;
import static org.geovistory.toolbox.streams.entity.label3.lib.Fn.*;
import static org.geovistory.toolbox.streams.entity.label3.names.Constants.DEFAULT_PROJECT;


public class CreateEntityLabel implements Processor<String, LabelEdge, ProjectLabelGroupKey, EntityLabelOperation> {
    private static final Logger LOG = LoggerFactory.getLogger(CreateEntityLabel.class);

    private ProcessorContext<ProjectLabelGroupKey, EntityLabelOperation> context;
    private KeyValueStore<ProjectEntityKey, EntityLabel> entityLabelStore;
    private KeyValueStore<String, LabelEdge> labelEdgeBySourceStore;
    private KeyValueStore<ProjectClassKey, EntityLabelConfigTmstp> labelConfigStore;
    private KeyValueStore<ProjectClassKey, Long> labelConfigTmspStore;
    private KeyValueStore<String, EntityLabel> comLabelRankStore;
    private KeyValueStore<String, EntityLabel> comLabelLangRankStore;
    private KeyValueStore<ComLabelGroupKey, Integer> comLabelCountStore;

    private KeyValueStore<String, Boolean> entityPublicationStore;

    private Boolean punctuationProcessing = false;

    private int punctuationCount = 0;

    public void init(ProcessorContext<ProjectLabelGroupKey, EntityLabelOperation> context) {
        entityLabelStore = context.getStateStore(EntityLabelStore.NAME);
        labelEdgeBySourceStore = context.getStateStore(LabelEdgeBySourceStore.NAME);
        labelConfigStore = context.getStateStore(GlobalLabelConfigStore.NAME);
        labelConfigTmspStore = context.getStateStore(LabelConfigTmstpStore.NAME);
        comLabelRankStore = context.getStateStore(ComLabelRankStore.NAME);
        comLabelLangRankStore = context.getStateStore(ComLabelLangRankStore.NAME);
        comLabelCountStore = context.getStateStore(ComLabelCountStore.NAME);
        entityPublicationStore = context.getStateStore(EntityPublicationStore.NAME);

        this.context = context;

        context.schedule(Duration.ofSeconds(1), PunctuationType.WALL_CLOCK_TIME, timestamp -> {

            LOG.debug("punctuation #{} called", this.punctuationCount);
            if (!punctuationProcessing) {
                LOG.debug("punctuation #{} processing", this.punctuationCount);
                punctuationProcessing = true;
                try (var iterator = labelConfigStore.all()) {
                    while (iterator.hasNext()) {
                        var item = iterator.next();
                        var oldT = labelConfigTmspStore.get(item.key);
                        var newVal = item.value;

                        var newT = newVal.getRecordTimestamp();
                        if (oldT == null || oldT < newT) {
                            LOG.debug("punctuation #{}, new label config t {}, old label config t {}", this.punctuationCount, newT, oldT);

                            var defaultConfig = item.key.getProjectId() == DEFAULT_PROJECT.get();

                            // create prefix
                            var prefix = defaultConfig ?
                                    createLabelEdgePrefix1(item.key.getClassId()) :
                                    createLabelEdgePrefix2(item.key.getClassId(), item.key.getProjectId());

                            LOG.debug("punctuation #{}, scan edges with prefix {}", this.punctuationCount, prefix);

                            // update labels with the new config
                            try (var i = this.labelEdgeBySourceStore.prefixScan(prefix, Serdes.String().serializer())) {
                                String previousGroupId = null;
                                // iterate over edges
                                while (i.hasNext()) {
                                    var r = i.next();
                                    var groupId = createSubstring(r.key);
                                    // in case we enter a new field defined by classId_projectId_sourceId
                                    if (!Objects.equals(groupId, previousGroupId)) {
                                        LOG.debug("punctuation #{}, update label of entity with classId_projectId_sourceId: {}", this.punctuationCount, groupId);

                                        var record = new Record<>(r.key, r.value, timestamp);
                                        this.process(record);
                                        previousGroupId = groupId;
                                    }
                                }
                            }
                        }
                        // set timestamp
                        labelConfigTmspStore.put(item.key, newT);
                    }
                }
                punctuationProcessing = false;
            }

            LOG.debug("punctuation #{} done", this.punctuationCount);
            this.punctuationCount++;
        });
    }


    public void process(Record<String, LabelEdge> record) {
        LOG.debug("process() called with record: {}", record);

        if (record.value() == null) return;
        EntityLabel newEntityLabel = null;

        // create source entity key based on edge
        var projectEntityKey = createProjectSourceEntityKey(record.value());

        // get old entity label
        EntityLabel oldEntityLabeL = entityLabelStore.get(projectEntityKey);

        // lookup entity label config
        EntityLabelConfigTmstp conf;
        try {
            conf = lookupEntityLabelConfig(record);
        } catch (NewLabelConfigFoundException ignore) {
            // in case a new label config has been found, we return here, since the label
            // will be created in batch mode
            return;
        }
        if (conf != null) {
            // create entity label
            newEntityLabel = createEntityLabel(record.value(), conf);
        }

        // if old and new are different...
        if (!Objects.equals(oldEntityLabeL, newEntityLabel)) {
            // ... update store
            entityLabelStore.put(projectEntityKey, newEntityLabel);

            // ... push downstream
            context.forward(
                    record
                            .withKey(createProjectLabelGroupKey(projectEntityKey))
                            .withValue(createEntityLabelOperation(newEntityLabel, false)),
                    Processors.RE_KEY_ENTITY_LABELS
            );

            LOG.debug("new label for {}: {}", projectEntityKey, newEntityLabel);

            // create community entity label
            if (projectEntityKey.getProjectId() != 0) {
                createCommunityLabels(projectEntityKey, newEntityLabel, oldEntityLabeL, record);
            }
        } else {
            LOG.debug("no new label for {}: {}", projectEntityKey, oldEntityLabeL);
        }

        // create output for toolbox project rdfs:label
        createOutputForRdf(record, PubTargets.TP, oldEntityLabeL, newEntityLabel);

        // create output for public project rdfs:label
        createOutputForRdf(record, PubTargets.PP, oldEntityLabeL, newEntityLabel);

    }


    public void createOutputForRdf(
            Record<String, LabelEdge> record,
            PubTargets pubTarget,
            EntityLabel oldLabel,
            EntityLabel newLabel
    ) {
        var visibleInPubTarget = visibleInPublicationTarget(record.value(), pubTarget);
        var publishedKey = getPublishedKey(record.value(), pubTarget);
        Boolean published = entityPublicationStore.get(publishedKey);
        published = published != null ? published : false;
        String childName = getChildName(pubTarget);

        // do delete
        if (published && (!visibleInPubTarget || newLabel == null) && oldLabel != null) {
            // track as not published
            entityPublicationStore.delete(publishedKey);

            // send delete message for old label
            context.forward(record
                    .withKey(createProjectLabelGroupKey(record.value(), oldLabel, pubTarget))
                    .withValue(createEntityLabelOperation(oldLabel, true)), childName);

        }
        // do update
        else if (published && !Objects.equals(oldLabel, newLabel) && oldLabel != null) {
            // send delete message for old label
            context.forward(record
                    .withKey(createProjectLabelGroupKey(record.value(), oldLabel, pubTarget))
                    .withValue(createEntityLabelOperation(oldLabel, true)), childName);

            // send insert message for new label
            context.forward(record
                    .withKey(createProjectLabelGroupKey(record.value(), newLabel, pubTarget))
                    .withValue(createEntityLabelOperation(newLabel, false)), childName);
        }
        // do insert
        else if (visibleInPubTarget && newLabel != null) {
            // track as published
            entityPublicationStore.put(publishedKey, true);

            // send insert message for new label
            context.forward(record
                    .withKey(createProjectLabelGroupKey(record.value(), newLabel, pubTarget))
                    .withValue(createEntityLabelOperation(newLabel, false)), childName);
        }


    }

    private static String getPublishedKey(LabelEdge l, PubTargets pubTarget) {
        return EntityPublicationStore.createKey(pubTarget, l.getProjectId(), l.getSourceId());
    }


    /**
     * Creates an EntityLabel based on the given record and configuration by looking up the
     * label edges store.
     *
     * @param labelEdge The labelEdge containing label edge information.
     * @param conf      The configuration for project entity labels.
     * @return An EntityLabel object.
     */
    private EntityLabel createEntityLabel(
            LabelEdge labelEdge,
            EntityLabelConfigTmstp conf
    ) {
        var strings = new ArrayList<String>();
        var languages = new HashSet<String>();
        for (EntityLabelConfigPart part : conf.getConfig().getLabelParts()) {
            var f = part.getField();
            var prefix = createPrefixForEdgeScan(labelEdge, f);
            // lookup edges of field
            try (var iterator = labelEdgeBySourceStore.prefixScan(prefix, Serdes.String().serializer())) {
                var i = 0;
                while (iterator.hasNext() && i < f.getNrOfStatementsInLabel()) {
                    i++;
                    var e = iterator.next();
                    var lab = e.value.getTargetLabel();
                    var lan = e.value.getTargetLabelLanguage();
                    if (lab != null) strings.add(lab.trim());
                    if (lan != null) languages.add(lan);
                }
            }
        }

        if (strings.size() == 0) return null;

        String lang;
        if (isAppellationInLanguage(labelEdge)) lang = getLanguage(labelEdge);
        else if (languages.size() == 1) lang = languages.stream().toList().get(0);
        else if (languages.size() > 1) lang = "mixed";
        else lang = "unknown";

        String label = String.join(", ", strings);
        if (label.length() >= 100) label = label.substring(0, 100);
        return EntityLabel.newBuilder().setLabel(label).setLanguage(lang).build();
    }

    private Boolean isAppellationInLanguage(LabelEdge e) {
        var sourceClass = e.getSourceClassId();
        return (sourceClass == 365 || sourceClass == 868);
    }

    private EntityLabelConfigTmstp lookupEntityLabelConfig(Record<String, LabelEdge> record) throws NewLabelConfigFoundException {
        var labelEdge = record.value();
        var projectClassKey = ProjectClassKey.newBuilder()
                .setProjectId(labelEdge.getProjectId())
                .setClassId(labelEdge.getSourceClassId()).build();
        var labelConfig = labelConfigStore.get(projectClassKey);

        if (labelConfig == null || labelConfig.getDeleted()) {
            projectClassKey.setProjectId(DEFAULT_PROJECT.get());
            labelConfig = labelConfigStore.get(projectClassKey);
        }

        if (labelConfig != null && !labelConfig.getDeleted()) return labelConfig;
        else return null;
    }

    public static String createPrefixForEdgeScan(LabelEdge e, EntityLabelConfigPartField f) {
        return createLabelEdgePrefix3(e.getSourceClassId(),
                e.getProjectId(),
                e.getSourceId(),
                f.getFkProperty(),
                f.getIsOutgoing());
    }

    private String getLanguage(LabelEdge e) {
        String langCode = "unknown";
        try (var iterator = this.labelEdgeBySourceStore.prefixScan(createPrefixLanguageScan(e), Serdes.String().serializer())) {
            if (iterator.hasNext()) {
                var item = iterator.next();
                try {
                    var parsedLangCode = Utils.getLanguageFromId(parseInt(item.value.getTargetId().replace("i", "")));
                    if (parsedLangCode != null) langCode = parsedLangCode;
                } catch (Exception ignore) {
                }
            }
        }
        return langCode;
    }

    public static String createPrefixLanguageScan(LabelEdge e) {
        return createLabelEdgePrefix3(e.getSourceClassId(),
                e.getProjectId(),
                e.getSourceId(),
                1112,
                true);
    }

    /**
     * Creates a substring containing the first three elements of the input string
     * along with the underscores between them: classId_projectId_sourceId_
     *
     * @param input The input string containing elements separated by underscores.
     * @return A substring containing the first three elements along with the underscores.
     */
    private String createSubstring(String input) {
        // Split the input string by underscores
        String[] parts = input.split("_");
        StringBuilder result = new StringBuilder();

        // Append the first three parts along with the underscores
        for (int i = 0; i < Math.min(3, parts.length); i++) {
            result.append(parts[i]);
            result.append("_");
        }

        return result.toString();
    }


    public void createCommunityLabels(
            ProjectEntityKey key,
            EntityLabel newEl,
            EntityLabel oldEl,
            Record<String, LabelEdge> record
    ) {
        LOG.debug("createCommunityLabels() called for {} with old label: '{}' and new label '{}'", key, oldEl, newEl);

        // get old preferred label
        var oldPrefLabel = getPreferredLabel(key.getEntityId());

        var comEntityKey = createComEntityKey(key.getEntityId());

        // if oldEl not null and different from newEl ...
        if (oldEl != null && !oldEl.equals(newEl)) {

            // get old preferred language label
            var oldPrefLangLabel = getPreferredLangLabel(key.getEntityId(), oldEl.getLanguage());

            // generate old group key {entity_id}_{label}_{language}
            var oldGroupKey = entityLabelToGroupKey(key.getEntityId(), oldEl);

            // ... decrease count of oldGroupKey
            var oldGroupCount = comLabelCountStore.get(oldGroupKey);

            // if oldGroupCount not null and bigger 0
            if (oldGroupCount != null && oldGroupCount > 0) {

                // generate rank key
                var rankKey = createRankKey(key.getEntityId(), oldEl, oldGroupCount);
                // delete this rank, since it will be replaced by the rank of newEl
                comLabelRankStore.delete(rankKey);

                // generate rank-in-lang-key
                var rankInLangKey = createRankInLangKey(key.getEntityId(), oldEl, oldGroupCount);
                // delete this rank-in-lang, since it will be replaced by the rank-in-lang-key of newEl
                comLabelLangRankStore.delete(rankInLangKey);

                // Decrease the count of the old group
                var c = oldGroupCount - 1;
                comLabelCountStore.put(oldGroupKey, c);

                // Generate the language label
                var newPrefLangLabel = getPreferredLangLabel(key.getEntityId(), oldEl.getLanguage());
                compareAndPushLangLabel(record, oldGroupKey, oldPrefLangLabel, newPrefLangLabel);
            }


        }
        // if newEl not null and different from oldEl
        if (newEl != null && !newEl.equals(oldEl)) {

            // get old preferred language label
            var oldPrefLangLabel = getPreferredLangLabel(key.getEntityId(), newEl.getLanguage());


            // generate new group key {entity_id}_{label}_{language}
            var newGroupKey = entityLabelToGroupKey(key.getEntityId(), newEl);

            // get count of new group
            var newGroupCount = comLabelCountStore.get(newGroupKey);

            // increase count of newGroupKey
            newGroupCount = newGroupCount != null ? (newGroupCount + 1) : 1;
            comLabelCountStore.put(newGroupKey, newGroupCount);

            // generate rank key
            var rankKey = createRankKey(key.getEntityId(), newEl, newGroupCount);
            // add this rank
            comLabelRankStore.put(rankKey, newEl);

            // generate rank-in-lang-key
            var rankInLangKey = createRankInLangKey(key.getEntityId(), newEl, newGroupCount);
            // delete this rank-in-lang, since it will be replaced by the rank-in-lang-key of newEl
            comLabelLangRankStore.put(rankInLangKey, newEl);

            // Generate the language label
            var newPrefLangLabel = getPreferredLangLabel(key.getEntityId(), newEl.getLanguage());
            compareAndPushLangLabel(record, newGroupKey, oldPrefLangLabel, newPrefLangLabel);
        }

        // Generate the preferred label
        var newPrefLabel = getPreferredLabel(key.getEntityId());

        // if oldPrefLabel and newPrefLabel distinct
        if (!Objects.equals(oldPrefLabel, newPrefLabel)) {
            LOG.debug("update community label for {} with old label: '{}' and new label '{}'", key, oldPrefLabel, newPrefLabel);
            context.forward(
                    record
                            .withKey(createProjectLabelGroupKey(comEntityKey))
                            .withValue(createEntityLabelOperation(newPrefLabel, false)),
                    Processors.RE_KEY_ENTITY_LABELS);
        }

        // Generate output for toolbox community rdf
        createOutputForRdf(record, PubTargets.TC, oldPrefLabel, newPrefLabel);

        // Generate output for public community rdf
        createOutputForRdf(record, PubTargets.PC, oldPrefLabel, newPrefLabel);
    }

    private void compareAndPushLangLabel(
            Record<String, LabelEdge> record,
            ComLabelGroupKey groupKey,
            EntityLabel oldLabel,
            EntityLabel newLabel) {
            /* TODO        if (!Objects.equals(oldLabel, newLabel))
            context.forward(
                    record.withKey(createProjectLabelGroupKey(groupKey))
                            .withValue(newLabel),
                    Processors.RE_KEY_ENTITY_LANG_LABELS);*/
    }

    /**
     * Retrieves the preferred label associated with the specified {@code entityId}.
     * The preferred label is obtained through a prefix scan of the provided entity ID in the {@code comLabelRankStore}.
     *
     * @param entityId The ID of the entity for which the preferred label is to be retrieved.
     * @return A {@code EntityLabel} representing the preferred label.
     * If no preferred label is found, {@code null} is returned.
     * @throws NullPointerException if the {@code entityId} is {@code null}.
     */
    private EntityLabel getPreferredLabel(String entityId) {
        EntityLabel l = null;
        try (var iterator = comLabelRankStore.prefixScan(entityId + "_", Serdes.String().serializer())) {
            if (iterator.hasNext()) {
                var item = iterator.next();
                if (item != null) l = item.value;
            }
        }
        return l;
    }

    /**
     * Retrieves the preferred language label associated with the specified {@code entityId}.
     * The preferred language label is obtained through a prefix scan of the provided entity ID in the {@code comLabelLangRankStore}.
     *
     * @param entityId The ID of the entity for which the preferred language label is to be retrieved.
     * @return A {@code EntityLabel} representing the preferred language label.
     * If no preferred language label is found, {@code null} is returned.
     * @throws NullPointerException if the {@code entityId} is {@code null}.
     */
    private EntityLabel getPreferredLangLabel(String entityId, String language) {
        EntityLabel l = null;
        try (var iterator = comLabelLangRankStore.prefixScan(entityId + "_" + language + "_", Serdes.String().serializer())) {
            if (iterator.hasNext()) {
                var item = iterator.next();
                if (item != null) l = item.value;
            }
        }
        return l;
    }

    /**
     * Converts an entity ID and an entity label into a {@code ComLabelGroupKey}.
     *
     * @param entityId    The ID of the entity.
     * @param entityLabel The label and language of the entity.
     * @return A {@code ComLabelGroupKey} representing the combination of the entity ID, label, and language.
     * @throws NullPointerException if either {@code entityId} or {@code entityLabel} is {@code null}.
     */
    private ComLabelGroupKey entityLabelToGroupKey(String entityId, EntityLabel entityLabel) {
        if (entityId == null) throw new NullPointerException("entityId must not be null");
        if (entityLabel == null) throw new NullPointerException("entityLabel must not be null");
        return ComLabelGroupKey.newBuilder()
                .setEntityId(entityId)
                .setLabel(entityLabel.getLabel())
                .setLanguage(entityLabel.getLanguage()).build();
    }

    /**
     * Creates a rank key based on the provided entity ID, entity label, and count.
     * The generated string has this form:
     * {entity_id}_{1/count}_{label}_{language}
     * <p>
     * This allows lexicographical sorting, where higher counts come first.
     *
     * @param entityId    The ID of the entity.
     * @param entityLabel The label and language of the entity.
     * @param count       The count associated with the entity.
     * @return A {@code String} representing the rank key.
     * @throws NullPointerException if either {@code entityId} or {@code entityLabel} is {@code null}.
     */
    private String createRankKey(String entityId, EntityLabel entityLabel, int count) {
        if (entityId == null) throw new NullPointerException("entityId must not be null");
        if (entityLabel == null) throw new NullPointerException("entityLabel must not be null");
        if (count < 1) throw new RuntimeException("Count must be bigger than 0");
        return entityId + "_" + (1f / count) + "_" + entityLabel.getLabel() + "_" + entityLabel.getLanguage();
    }


    /**
     * Creates a rank-in-lang-key based on the provided entity ID, entity label, and count.
     * The generated string has this form:
     * {entity_id}_{language}_{1/count}_{label}
     * <p>
     * This allows lexicographical sorting, where higher counts come first.
     *
     * @param entityId    The ID of the entity.
     * @param entityLabel The label and language of the entity.
     * @param count       The count associated with the entity.
     * @return A {@code String} representing the rank key.
     * @throws NullPointerException if either {@code entityId} or {@code entityLabel} is {@code null}.
     */
    private String createRankInLangKey(String entityId, EntityLabel entityLabel, int count) {
        if (entityId == null) throw new NullPointerException("entityId must not be null");
        if (entityLabel == null) throw new NullPointerException("entityLabel must not be null");
        if (count < 1) throw new RuntimeException("Count must be bigger than 0");
        return entityId + "_" + entityLabel.getLanguage() + "_" + (1f / count) + "_" + entityLabel.getLabel();
    }

    /**
     * Creates a {@code ProjectEntityKey} based on the provided entity ID
     * with project ID = 0.
     *
     * @param entityId The ID of the entity.
     * @return A {@code ProjectEntityKey} representing the entity with the provided ID and a default project ID of 0.
     * @throws NullPointerException if {@code entityId} is {@code null}.
     */
    private ProjectEntityKey createComEntityKey(String entityId) {
        return ProjectEntityKey.newBuilder()
                .setEntityId(entityId)
                .setProjectId(0).build();
    }

    /**
     * Thrown if the label config has been updated meanwhile.
     */
    private static class NewLabelConfigFoundException extends RuntimeException {
    }
}