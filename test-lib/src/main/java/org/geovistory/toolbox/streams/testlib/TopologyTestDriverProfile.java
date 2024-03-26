package org.geovistory.toolbox.streams.testlib;

import io.quarkus.test.junit.QuarkusTestProfile;

import java.util.Map;

public class TopologyTestDriverProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
        return Map.of(
                "auto.create.output.topics", "disabled",
                "schema.registry.url", "mock://schema-registry"
        );
    }
}