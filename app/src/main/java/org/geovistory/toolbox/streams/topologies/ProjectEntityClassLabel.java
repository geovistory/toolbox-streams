package org.geovistory.toolbox.streams.topologies;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.app.RegisterOutputTopic;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.Utils;


public class ProjectEntityClassLabel {

    public static void main(String[] args) {
        System.out.println(buildStandalone(new StreamsBuilder()).describe());
    }

    public static Topology buildStandalone(StreamsBuilder builder) {
        var registerOutputTopic = new RegisterOutputTopic(builder);

        return addProcessors(
                builder,
                registerOutputTopic.projectEntityTable(),
                registerOutputTopic.projectClassLabelTable()
        ).builder().build();
    }

    public static ProjectEntityClassLabelLabelReturnValue addProcessors(
            StreamsBuilder builder,
            KTable<ProjectEntityKey, ProjectEntityValue> projectEntityTable,
            KTable<ProjectClassLabelKey, ProjectClassLabelValue> projectClassLabelTable
    ) {

        var avroSerdes = new ConfluentAvroSerdes();


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
                Materialized.<ProjectEntityKey, ProjectEntityClassLabelValue, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.project_entity_with_class_label)
                        .withKeySerde(avroSerdes.ProjectEntityKey())
                        .withValueSerde(avroSerdes.ProjectEntityClassLabelValue())
        );


        var projectEntityClassLabelStream = projectEntityClassLabelTable.toStream();
        /* SINK PROCESSORS */

        projectEntityClassLabelStream.to(output.TOPICS.project_entity_class_label,
                Produced.with(avroSerdes.ProjectEntityKey(), avroSerdes.ProjectEntityClassLabelValue()));

        return new ProjectEntityClassLabelLabelReturnValue(builder, projectEntityClassLabelStream);

    }


    public enum input {
        TOPICS;
        public final String project_entity = ProjectEntity.output.TOPICS.project_entity;
        public final String project_class_label = ProjectClassLabel.output.TOPICS.project_class_label;
    }


    public enum inner {
        TOPICS;
        public final String project_entity_with_class_label = "project_entity_with_class_label";

    }

    public enum output {
        TOPICS;
        public final String project_entity_class_label = Utils.tsPrefixed("project_entity_class_label");
    }


}
