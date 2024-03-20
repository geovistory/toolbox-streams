package org.geovistory.toolbox.streams.entity.label3.processors;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.entity.label3.stores.EntityLabelStore;
import org.geovistory.toolbox.streams.entity.label3.stores.GlobalLabelConfigStore;
import org.geovistory.toolbox.streams.entity.label3.stores.LabelConfigTmstpStore;
import org.geovistory.toolbox.streams.entity.label3.stores.LabelEdgeBySourceStore;
import org.geovistory.toolbox.streams.lib.Utils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Objects;

import static java.lang.Integer.parseInt;
import static org.geovistory.toolbox.streams.entity.label3.lib.Fn.*;
import static org.geovistory.toolbox.streams.entity.label3.names.Constants.DEFAULT_PROJECT;


public class CreateEntityLabel implements Processor<String, LabelEdge, ProjectEntityKey, EntityLabel> {
    private ProcessorContext<ProjectEntityKey, EntityLabel> context;
    private KeyValueStore<ProjectEntityKey, EntityLabel> entityLabelStore;
    private KeyValueStore<String, LabelEdge> labelEdgeBySourceStore;
    private KeyValueStore<ProjectClassKey, EntityLabelConfigTmstp> labelConfigStore;
    private KeyValueStore<ProjectClassKey, Long> labelConfigTmspStore;

    private Boolean punctuationProcessing = false;

    public void init(ProcessorContext<ProjectEntityKey, EntityLabel> context) {
        entityLabelStore = context.getStateStore(EntityLabelStore.NAME);
        labelEdgeBySourceStore = context.getStateStore(LabelEdgeBySourceStore.NAME);
        labelConfigStore = context.getStateStore(GlobalLabelConfigStore.NAME);
        labelConfigTmspStore = context.getStateStore(LabelConfigTmstpStore.NAME);
        this.context = context;

        context.schedule(Duration.ofSeconds(1), PunctuationType.WALL_CLOCK_TIME, timestamp -> {
            if (!punctuationProcessing) {
                punctuationProcessing = true;
                try (var iterator = labelConfigStore.all()) {
                    while (iterator.hasNext()) {
                        var item = iterator.next();
                        var oldT = labelConfigTmspStore.get(item.key);
                        var newVal = item.value;

                        var newT = newVal.getRecordTimestamp();
                        if (oldT == null || oldT < newT) {
                            var defaultConfig = item.key.getProjectId() == DEFAULT_PROJECT.get();

                            // create prefix
                            var prefix = defaultConfig ?
                                    createLabelEdgePrefix1(item.key.getClassId()) :
                                    createLabelEdgePrefix2(item.key.getClassId(), item.key.getProjectId());

                            // update labels with the new config
                            updateLabelsWithNewConfig(prefix, newT);
                        }
                    }
                }
                punctuationProcessing = false;
            }
        });
    }


    public void process(Record<String, LabelEdge> record) {
        if (record.value() == null) return;
        EntityLabel newEntityLabel = null;

        // create source entity key based on edge
        var projectEntityKey = createProjectSourceEntityKey(record.value());

        // get old entity label
        EntityLabel oldEntityLabeL = entityLabelStore.get(projectEntityKey);

        // lookup entity label config
        var conf = lookupEntityLabelConfig(record);
        if (conf != null) {
            // create entity label
            newEntityLabel = createEntityLabel(record.value(), conf);
        }

        // if old and new are different...
        if (!Objects.equals(oldEntityLabeL, newEntityLabel)) {
            // ... update store
            entityLabelStore.put(projectEntityKey, newEntityLabel);

            // ... push downstream
            context.forward(record.withKey(projectEntityKey).withValue(newEntityLabel));
        }

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
                    if (lab != null) strings.add(lab);
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
        return EntityLabel.newBuilder().setLabel(label).setLanguage(lang).build();
    }

    private Boolean isAppellationInLanguage(LabelEdge e) {
        var sourceClass = e.getSourceClassId();
        return (sourceClass == 365 || sourceClass == 868);
    }

    private EntityLabelConfigTmstp lookupEntityLabelConfig(Record<String, LabelEdge> record) {
        var labelEdge = record.value();
        var projectClassKey = ProjectClassKey.newBuilder()
                .setProjectId(labelEdge.getProjectId())
                .setClassId(labelEdge.getSourceClassId()).build();
        var labelConfig = labelConfigStore.get(projectClassKey);

        if (labelConfig != null && !labelConfig.getDeleted()) {
            // lookup old timestamp
            var oldT = labelConfigTmspStore.get(projectClassKey);
            var newR = labelConfig.getRecordTimestamp();
            if (oldT != null && oldT < newR) {
                labelConfigTmspStore.put(projectClassKey, newR);
                // update labels of all entities of this class and project
                updateLabelsWithNewConfig(
                        createLabelEdgePrefix2(projectClassKey.getClassId(), projectClassKey.getProjectId()),
                        record.timestamp()
                );
                return null;
            }
        } else {
            projectClassKey.setProjectId(DEFAULT_PROJECT.get());
            labelConfig = labelConfigStore.get(projectClassKey);
            if (labelConfig != null && !labelConfig.getDeleted()) {
                var oldT2 = labelConfigTmspStore.get(projectClassKey);
                var newR2 = labelConfig.getRecordTimestamp();
                if (oldT2 != null && oldT2 < newR2) {
                    labelConfigTmspStore.put(projectClassKey, newR2);
                    // update labels of all entities of this class
                    updateLabelsWithNewConfig(
                            createLabelEdgePrefix1(projectClassKey.getClassId()),
                            record.timestamp()
                    );
                    return null;
                }
            }
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


    private void updateLabelsWithNewConfig(String prefix, Long timestamp) {
        // iterate over edges
        try (var iterator = this.labelEdgeBySourceStore.prefixScan(prefix, Serdes.String().serializer())) {

            String previousGroupId = null;
            LabelEdge labelEdge;
            while (iterator.hasNext()) {
                var r = iterator.next();
                var groupId = createSubstring(r.key);
                labelEdge = r.value;
                // in case we enter a new field defined by classId_projectId_sourceId
                if (!Objects.equals(groupId, previousGroupId)) {
                    var record = new Record<>(r.key, labelEdge, timestamp);
                    this.process(record);
                    previousGroupId = groupId;
                }
            }
        }
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


}