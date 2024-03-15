package lib;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import org.testcontainers.redpanda.RedpandaContainer;

import java.util.HashMap;
import java.util.Map;

public class RedpandaResource implements QuarkusTestResourceLifecycleManager {

    final public RedpandaContainer container = new RedpandaContainer("docker.redpanda.com/redpandadata/redpanda:v23.1.2");

    @Override
    public Map<String, String> start() {
        container.start();
        var props = new HashMap<String, String>();
        props.put("kafka.bootstrap.servers", container.getBootstrapServers());
        props.put("schema.registry.url", container.getSchemaRegistryAddress());

        return props;
    }


    @Override
    public void stop() {

        container.close();
    }


    @Override
    public void inject(TestInjector testInjector) {
        testInjector.injectIntoFields(this, new TestInjector.AnnotatedAndMatchesType(InjectRedpandaResource.class, RedpandaResource.class));
    }

    public String getBootstrapServers() {
        return container.getBootstrapServers();
    }

    public String getSchemaRegistryAddress() {
        return container.getSchemaRegistryAddress();
    }


}
