package org.geovistory.toolbox.streams.entity.label.setup;

import io.quarkus.runtime.StartupEvent;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.lib.TopicNameEnum;
import org.geovistory.toolbox.streams.lib.TsAdmin;
import org.geovistory.toolbox.streams.lib.Utils;
import org.jboss.logging.Logger;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import java.util.ArrayList;

@ApplicationScoped
public class SetupTopics {
    private static final Logger LOG = Logger.getLogger(SetupTopics.class);

    @ConfigProperty(name = "ts.output.topic.partitions")
    int outputTopicPartitions;
    @ConfigProperty(name = "ts.output.topic.replication.factor")
    short outputTopicReplicationFactor;
    @ConfigProperty(name = "quarkus.kafka.streams.bootstrap.servers")
    String bootstrapServers;

    @ConfigProperty(name = "ts.input.topic.name.prefix", defaultValue = "")
    String prefix;

    @ConfigProperty(name = "ts.topic.statement.with.entity", defaultValue = "")
    String statementWithEntity = "statement.with.entity";
    @ConfigProperty(name = "ts.topic.statement.with.literal", defaultValue = "")
    String statementWithLiteral = "statement.with.literal";
    @ConfigProperty(name = "ts.topic.project.entity.label.config", defaultValue = "")
    String projectEntityLabelConfig = "project.entity.label.config";
    @ConfigProperty(name = "ts.topic.community.entity.label.config", defaultValue = "")
    String communityEntityLabelConfig = "community.entity.label.config";

    public String getStatementWithEntity() {
        return statementWithEntity;
    }

    public String getStatementWithLiteral() {
        return statementWithLiteral;
    }

    public String getProjectEntityLabelConfig() {
        return projectEntityLabelConfig;
    }

    public String getCommunityEntityLabelConfig() {
        return communityEntityLabelConfig;
    }

    public String proInfoProjRel() {
        return Utils.prefixedIn(prefix, TopicNameEnum.pro_info_proj_rel.getValue());
    }

    public String infResource() {
        return Utils.prefixedIn(prefix, TopicNameEnum.inf_resource.getValue());
    }

    void onStart(@Observes StartupEvent ev) {
        LOG.info("The application is starting...");

        var admin = new TsAdmin(bootstrapServers);
        // create output topics (with number of partitions and delete.policy=compact)
        var topics = new ArrayList<String>();
        topics.add(getStatementWithEntity());
        topics.add(getStatementWithLiteral());
        topics.add(getProjectEntityLabelConfig());
        topics.add(getCommunityEntityLabelConfig());
        topics.add(proInfoProjRel());
        topics.add(infResource());

        admin.createOrConfigureTopics(topics, outputTopicPartitions, outputTopicReplicationFactor);
    }


}