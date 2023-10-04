package org.geovistory.toolbox.streams.entity.processors.project;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.entity.AvroSerdes;
import org.geovistory.toolbox.streams.entity.OutputTopicNames;
import org.geovistory.toolbox.streams.entity.RegisterInputTopic;
import org.geovistory.toolbox.streams.lib.Utils;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped

public class ProjectEntityClassLabel {


    AvroSerdes avroSerdes;

    @Inject
    RegisterInputTopic registerInputTopic;

    @Inject
    OutputTopicNames outputTopicNames;

    public ProjectEntityClassLabel(AvroSerdes avroSerdes, RegisterInputTopic registerInputTopic, OutputTopicNames outputTopicNames) {
        this.avroSerdes = avroSerdes;
        this.registerInputTopic = registerInputTopic;
        this.outputTopicNames = outputTopicNames;
    }

    public void addProcessorsStandalone() {
        addProcessors(
                registerInputTopic.projectEntityTable(),
                registerInputTopic.projectClassLabelTable()
        );
    }

    public ProjectEntityClassLabelReturnValue addProcessors(
            KTable<ProjectEntityKey, ProjectEntityValue> projectEntityTable,
            KTable<ProjectClassLabelKey, ProjectClassLabelValue> projectClassLabelTable
    ) {

        /* STREAM PROCESSORS */
        // 2)

        var projectEntityClassLabelTable = projectEntityTable.join(
                projectClassLabelTable,
                projectEntityValue -> ProjectClassLabelKey.newBuilder()
                        .setClassId(projectEntityValue.getClassId())
                        .setProjectId(projectEntityValue.getProjectId())
                        .build(),
                (value1, value2) -> ProjectEntityClassLabelValue.newBuilder()
                        .setEntityId(value1.getEntityId())
                        .setProjectId(value1.getProjectId())
                        .setClassId(value1.getClassId())
                        .setClassLabel(value2.getLabel())
                        .setDeleted$1(Utils.includesTrue(value1.getDeleted$1(), value2.getDeleted$1()))
                        .build(),
                TableJoined.as(inner.TOPICS.project_entity_with_class_label + "-fk-join"),
                Materialized.<ProjectEntityKey, ProjectEntityClassLabelValue, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.project_entity_with_class_label)
                        .withKeySerde(avroSerdes.ProjectEntityKey())
                        .withValueSerde(avroSerdes.ProjectEntityClassLabelValue())
        );


        var projectEntityClassLabelStream = projectEntityClassLabelTable.toStream(
                Named.as(inner.TOPICS.project_entity_with_class_label + "-to-stream")
        );
        /* SINK PROCESSORS */

        projectEntityClassLabelStream.to(outputTopicNames.projectEntityClassLabel(),
                Produced.with(avroSerdes.ProjectEntityKey(), avroSerdes.ProjectEntityClassLabelValue())
                        .withName(outputTopicNames.projectEntityClassLabel() + "-producer")
        );

        return new ProjectEntityClassLabelReturnValue(projectEntityClassLabelTable, projectEntityClassLabelStream);

    }


    public enum inner {
        TOPICS;
        public final String project_entity_with_class_label = "project_entity_with_class_label";

    }


}
