package org.geovistory.toolbox.streams.lib.jsonmodels;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

public class SysConfigValueTest {
    ObjectMapper mapper = new ObjectMapper(); // create once, reuse

    @Test
    void readFile() {
        var f = new File("./src/test/java/org/geovistory/toolbox/streams/lib/jsonmodels/SysConfigValue.json");
        assertThat(f.exists()).isTrue();
    }

    @Test
    void testDecoding() throws IOException {
        SysConfigValue value = mapper.readValue(new File("./src/test/java/org/geovistory/toolbox/streams/lib/jsonmodels/SysConfigValue.json"), SysConfigValue.class);

        assertThat(value.ontome.requiredOntomeProfiles).isNotNull();
        assertThat(value.ontome.requiredOntomeProfiles).isNotNull();
    }

    @Test
    void testDecodingRequiredOntomeProfiles() throws IOException {
        String json = "{\"ontome\": {\"requiredOntomeProfiles\": [5, 97]}}";
        SysConfigValue value = mapper.readValue(json, SysConfigValue.class);
        assertThat(value.ontome.requiredOntomeProfiles.get(0)).isEqualTo(5);
        assertThat(value.ontome.requiredOntomeProfiles.get(1)).isEqualTo(97);
    }
}

