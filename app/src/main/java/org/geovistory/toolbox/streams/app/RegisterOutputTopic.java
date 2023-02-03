
package org.geovistory.toolbox.streams.app;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.topologies.*;

/**
 * This class provides helper methods to register
 * output topics (generated by this app).
 * These helper methods are mainly used for testing.
 */
public class RegisterOutputTopic {
    public StreamsBuilder builder;
    public ConfluentAvroSerdes avroSerdes;

    public RegisterOutputTopic(StreamsBuilder builder) {
        this.builder = builder;
        this.avroSerdes = new ConfluentAvroSerdes();
    }

    public KStream<ProjectProfileKey, ProjectProfileValue> projectProfileStream() {
        return builder.stream(ProjectProfiles.output.TOPICS.project_profile,
                Consumed.with(avroSerdes.ProjectProfileKey(), avroSerdes.ProjectProfileValue()));
    }

    public KStream<OntomeClassLabelKey, OntomeClassLabelValue> ontomeClassLabelStream() {
        return builder.stream(OntomeClassLabel.output.TOPICS.ontome_class_label,
                Consumed.with(avroSerdes.OntomeClassLabelKey(), avroSerdes.OntomeClassLabelValue()));
    }

    public KStream<GeovClassLabelKey, GeovClassLabelValue> geovClassLabelStream() {
        return builder.stream(GeovClassLabel.output.TOPICS.geov_class_label,
                Consumed.with(avroSerdes.GeovClassLabelKey(), avroSerdes.GeovClassLabelValue()));
    }

    public KStream<ProjectClassKey, ProjectClassValue> projectClassStream() {
        return builder.stream(ProjectClass.output.TOPICS.project_class,
                Consumed.with(avroSerdes.ProjectClassKey(), avroSerdes.ProjectClassValue()));
    }

    public KTable<ProjectClassKey, ProjectClassValue> projectClassTable() {
        return builder.table(ProjectClass.output.TOPICS.project_class,
                Consumed.with(avroSerdes.ProjectClassKey(), avroSerdes.ProjectClassValue()));
    }

    public KStream<OntomePropertyLabelKey, OntomePropertyLabelValue> ontomePropertyLabelStream() {
        return builder.stream(OntomePropertyLabel.output.TOPICS.ontome_property_label,
                Consumed.with(avroSerdes.OntomePropertyLabelKey(), avroSerdes.OntomePropertyLabelValue()));
    }

    public KStream<GeovPropertyLabelKey, GeovPropertyLabelValue> geovPropertyLabelStream() {
        return builder.stream(GeovPropertyLabel.output.TOPICS.geov_property_label,
                Consumed.with(avroSerdes.GeovPropertyLabelKey(), avroSerdes.GeovPropertyLabelValue()));
    }
    public KStream<ProjectPropertyKey, ProjectPropertyValue> projectPropertyStream() {
        return builder.stream(ProjectProperty.output.TOPICS.project_property,
                Consumed.with(avroSerdes.ProjectPropertyKey(), avroSerdes.ProjectPropertyValue()));
    }
    public KTable<ProjectEntityKey, ProjectEntityValue> projectEntityTable() {
        return builder.table(ProjectEntity.output.TOPICS.project_entity,
                Consumed.with(avroSerdes.ProjectEntityKey(), avroSerdes.ProjectEntityValue()));
    }

    public KTable<ProjectEntityKey, ProjectEntityTopStatementsValue> projectEntityTopStatementsTable() {
        return builder.table(ProjectEntityTopStatements.output.TOPICS.project_entity_top_statements,
                Consumed.with(avroSerdes.ProjectEntityKey(), avroSerdes.ProjectEntityTopStatementsValue()));
    }

    public KTable<dev.information.statement.Key, StatementEnrichedValue> statementEnrichedTable() {
        return builder.table(StatementEnriched.output.TOPICS.statement_enriched,
                Consumed.with(avroSerdes.InfStatementKey(), avroSerdes.StatementEnrichedValue()));

    }

    public KTable<ProjectStatementKey, ProjectStatementValue> projectStatementTable() {
        return builder.table(ProjectStatement.output.TOPICS.project_statement,
                Consumed.with(avroSerdes.ProjectStatementKey(), avroSerdes.ProjectStatementValue()));

    }

    public KTable<CommunityEntityLabelConfigKey, CommunityEntityLabelConfigValue> communityEntityLabelConfigTable() {
        return builder.table(CommunityEntityLabelConfig.output.TOPICS.community_entity_label_config,
                Consumed.with(avroSerdes.CommunityEntityLabelConfigKey(), avroSerdes.CommunityEntityLabelConfigValue()));
    }

    public KTable<ProjectClassKey, ProjectEntityLabelConfigValue> projectEntityLabelConfigTable() {
        return builder.table(ProjectEntityLabelConfig.output.TOPICS.project_entity_label_config_enriched,
                Consumed.with(avroSerdes.ProjectClassKey(), avroSerdes.ProjectEntityLabelConfigValue()));
    }

    public KTable<ProjectFieldLabelKey, ProjectFieldLabelValue> projectPropertyLabelTable() {
        return builder.table(ProjectPropertyLabel.output.TOPICS.project_property_label,
                Consumed.with(avroSerdes.ProjectPropertyLabelKey(), avroSerdes.ProjectPropertyLabelValue()));

    }
    public KStream<ProjectTopStatementsKey, ProjectTopStatementsValue> projectTopOutgoingStatementsStream() {
        return builder.stream(ProjectTopOutgoingStatements.output.TOPICS.project_top_outgoing_statements,
                Consumed.with(avroSerdes.ProjectTopStatementsKey(), avroSerdes.ProjectTopStatementsValue()));
    }

    public KStream<ProjectTopStatementsKey, ProjectTopStatementsValue> projectTopIncomingStatementsStream() {
        return builder.stream(ProjectTopIncomingStatements.output.TOPICS.project_top_incoming_statements,
                Consumed.with(avroSerdes.ProjectTopStatementsKey(), avroSerdes.ProjectTopStatementsValue()));
    }

    public KTable<ProjectTopStatementsKey, ProjectTopStatementsValue> projectTopStatementsTable() {
        return builder.table(ProjectTopStatements.output.TOPICS.project_top_statements,
                Consumed.with(avroSerdes.ProjectTopStatementsKey(), avroSerdes.ProjectTopStatementsValue()));
    }

    public KTable<ProjectTopStatementsKey, ProjectTopStatementsValue> projectTopOutgoingStatementsTable() {
        return builder.table(ProjectTopOutgoingStatements.output.TOPICS.project_top_outgoing_statements,
                Consumed.with(avroSerdes.ProjectTopStatementsKey(), avroSerdes.ProjectTopStatementsValue()));
    }

    public KTable<ProjectEntityKey, ProjectEntityLabelValue> projectEntityLabelTable() {
        return builder.table(ProjectEntityLabel.output.TOPICS.project_entity_label,
                Consumed.with(avroSerdes.ProjectEntityKey(), avroSerdes.ProjectEntityLabelValue()));
    }

    public KStream<ProjectEntityKey, ProjectEntityTopStatementsValue> projectEntityTopStatementsStream() {
        return builder.stream(ProjectEntityTopStatements.output.TOPICS.project_entity_top_statements,
                Consumed.with(avroSerdes.ProjectEntityKey(), avroSerdes.ProjectEntityTopStatementsValue()));
    }

    public KTable<HasTypePropertyKey, HasTypePropertyValue> hasTypePropertyTable() {
        return builder.table(HasTypeProperty.output.TOPICS.has_type_property,
                Consumed.with(avroSerdes.HasTypePropertyKey(), avroSerdes.HasTypePropertyValue()));
    }


    public KTable<ProjectClassLabelKey, ProjectClassLabelValue> projectClassLabelTable() {
        return builder.table(ProjectClassLabel.output.TOPICS.project_class_label,
                Consumed.with(avroSerdes.ProjectClassLabelKey(), avroSerdes.ProjectClassLabelValue()));
    }

    public KTable<ProjectEntityKey, ProjectEntityTypeValue> projectEntityTypeTable() {
        return builder.table(ProjectEntityType.output.TOPICS.project_entity_type,
                Consumed.with(avroSerdes.ProjectEntityKey(), avroSerdes.ProjectEntityTypeValue()));
    }
    public KTable<ProjectEntityKey, TimeSpanValue> projectEntityTimeSpanTable() {
        return builder.table(ProjectEntityTimeSpan.output.TOPICS.project_entity_time_span,
                Consumed.with(avroSerdes.ProjectEntityKey(), avroSerdes.TimeSpanValue()));
    }
    public KTable<ProjectEntityKey, ProjectEntityFulltextValue> projectEntityFulltextTable() {
        return builder.table(ProjectEntityFulltext.output.TOPICS.project_entity_fulltext,
                Consumed.with(avroSerdes.ProjectEntityKey(), avroSerdes.ProjectEntityFulltextValue()));
    }
    public KTable<ProjectEntityKey, ProjectEntityClassLabelValue> projectEntityClassLabelTable() {
        return builder.table(ProjectEntityClassLabel.output.TOPICS.project_entity_class_label,
                Consumed.with(avroSerdes.ProjectEntityKey(), avroSerdes.ProjectEntityClassLabelValue()));
    }

    public KTable<OntomeClassKey,OntomeClassMetadataValue> ontomeClassMetadataTable() {
        return builder.table(OntomeClassMetadata.output.TOPICS.ontome_class_metadata,
                Consumed.with(avroSerdes.OntomeClassKey(), avroSerdes.OntomeClassMetadataValue()));
    }

    public KTable<ProjectEntityKey,ProjectEntityClassMetadataValue> projectEntityClassMetadataTable() {
        return builder.table(ProjectEntityClassMetadata.output.TOPICS.project_entity_class_metadata,
                Consumed.with(avroSerdes.ProjectEntityKey(), avroSerdes.ProjectEntityClassMetadataValue()));
    }

}
