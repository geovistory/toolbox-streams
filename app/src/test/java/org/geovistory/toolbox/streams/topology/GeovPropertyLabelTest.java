package org.geovistory.toolbox.streams.topology;


import org.apache.kafka.streams.*;
import org.geovistory.toolbox.streams.avro.GeovPropertyLabelKey;
import org.geovistory.toolbox.streams.avro.GeovPropertyLabelValue;
import org.geovistory.toolbox.streams.lib.AppConfig;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.topologies.GeovPropertyLabel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class GeovPropertyLabelTest {

    private static final String SCHEMA_REGISTRY_SCOPE = GeovPropertyLabelTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private TopologyTestDriver testDriver;

    private TestInputTopic<dev.projects.text_property.Key, dev.projects.text_property.Value> textPropertyTopic;
    private TestOutputTopic<GeovPropertyLabelKey, GeovPropertyLabelValue> geovPropertyLabelTopic;


    @BeforeEach
    void setup() {


        Properties props = new Properties();
        var appId = "test";
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-test");
        AppConfig.INSTANCE.setSchemaRegistryUrl(MOCK_SCHEMA_REGISTRY_URL);

        Topology topology = GeovPropertyLabel.buildStandalone(new StreamsBuilder());

        testDriver = new TopologyTestDriver(topology, props);

        var avroSerdes = new ConfluentAvroSerdes();

        textPropertyTopic = testDriver.createInputTopic(
                GeovPropertyLabel.input.TOPICS.text_property,
                avroSerdes.ProTextPropertyKey().serializer(),
                avroSerdes.ProTextPropertyValue().serializer());


        geovPropertyLabelTopic = testDriver.createOutputTopic(
                GeovPropertyLabel.output.TOPICS.geov_property_label,
                avroSerdes.GeovPropertyLabelKey().deserializer(),
                avroSerdes.GeovPropertyLabelValue().deserializer());


    }

    @AfterEach
    void teardown() {
        testDriver.close();
    }

    @Test
    void testGeovPropertyLabel() {
        // add property label
        var k = new dev.projects.text_property.Key(1);
        var v = dev.projects.text_property.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setQuillDoc("")
                .setEntityVersion(0)
                .setFkProject(10)
                .setFkDfhProperty(20)
                .setFkDfhPropertyDomain(30)
                .setFkLanguage(18605)
                .setString("Property Label")
                .build();
        textPropertyTopic.pipeInput(k, v);

        // add property inverse label
        k= new dev.projects.text_property.Key(2);
        v = dev.projects.text_property.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setQuillDoc("")
                .setEntityVersion(0)
                .setFkProject(10)
                .setFkDfhProperty(20)
                .setFkDfhPropertyRange(40)
                .setFkLanguage(18605)
                .setString("Property Inverse Label")
                .build();
        textPropertyTopic.pipeInput(k, v);

        // add text_property not for property
        k.setPkEntity(2);
        v.setFkDfhProperty(null);
        v.setFkProProject(44);
        v.setString("Project Label");
        textPropertyTopic.pipeInput(k, v);


        assertThat(geovPropertyLabelTopic.isEmpty()).isFalse();
        var outRecords = geovPropertyLabelTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(2);
        var propertyLangKey = GeovPropertyLabelKey.newBuilder()
                .setProjectId(10)
                .setClassId(30)
                .setIsOutgoing(true)
                .setPropertyId(20)
                .setLanguageId(18605)
                .build();
        assertThat(outRecords.get(propertyLangKey).getLabel()).isEqualTo("Property Label");

        propertyLangKey = GeovPropertyLabelKey.newBuilder()
                .setProjectId(10)
                .setClassId(40)
                .setIsOutgoing(false)
                .setPropertyId(20)
                .setLanguageId(18605)
                .build();
        assertThat(outRecords.get(propertyLangKey).getLabel()).isEqualTo("Property Inverse Label");
    }

}
