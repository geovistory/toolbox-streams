
package org.geovistory.toolbox.streams.base.config;

import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.lib.TsRegisterInputTopic;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/**
 * This class provides helper methods to register
 * output topics (generated by this app).
 * These helper methods are mainly used for testing.
 */
@ApplicationScoped
public class RegisterInnerTopic extends TsRegisterInputTopic {

    @Inject
    AvroSerdes avroSerdes;
    @Inject
    public BuilderSingleton builderSingleton;
    @Inject
    OutputTopicNames outputTopicNames;

    public RegisterInnerTopic(AvroSerdes avroSerdes, BuilderSingleton builderSingleton, OutputTopicNames outputTopicNames) {
        this.avroSerdes = avroSerdes;
        this.builderSingleton = builderSingleton;
        this.outputTopicNames = outputTopicNames;
    }


    public KStream<ProjectProfileKey, ProjectProfileValue> projectProfileStream() {
        return getStream(
                builderSingleton.builder,
                outputTopicNames.projectProfile(),
                avroSerdes.ProjectProfileKey(), avroSerdes.ProjectProfileValue());
    }


    public KStream<GeovClassLabelKey, GeovClassLabelValue> geovClassLabelStream() {
        return getStream(
                builderSingleton.builder,
                outputTopicNames.geovClassLabel(),
                avroSerdes.GeovClassLabelKey(), avroSerdes.GeovClassLabelValue());
    }


    public KStream<ProjectClassKey, ProjectClassValue> projectClassStream() {
        return getStream(
                builderSingleton.builder,
                outputTopicNames.projectClass(),
                avroSerdes.ProjectClassKey(), avroSerdes.ProjectClassValue());
    }

    public KTable<ProjectClassKey, ProjectClassValue> projectClassTable() {
        return getTable(
                builderSingleton.builder,
                outputTopicNames.projectClass(),
                avroSerdes.ProjectClassKey(), avroSerdes.ProjectClassValue());
    }


    public KStream<GeovPropertyLabelKey, GeovPropertyLabelValue> geovPropertyLabelStream() {
        return getStream(
                builderSingleton.builder,
                outputTopicNames.geovPropertyLabel(),
                avroSerdes.GeovPropertyLabelKey(), avroSerdes.GeovPropertyLabelValue());
    }

    public KStream<ProjectPropertyKey, ProjectPropertyValue> projectPropertyStream() {
        return getStream(
                builderSingleton.builder,
                outputTopicNames.projectProperty(),
                avroSerdes.ProjectPropertyKey(), avroSerdes.ProjectPropertyValue());

    }

    public KTable<CommunityEntityLabelConfigKey, CommunityEntityLabelConfigValue> communityEntityLabelConfigTable() {
        return getTable(
                builderSingleton.builder,
                outputTopicNames.communityEntityLabelConfig(),
                avroSerdes.CommunityEntityLabelConfigKey(), avroSerdes.CommunityEntityLabelConfigValue());
    }


}
