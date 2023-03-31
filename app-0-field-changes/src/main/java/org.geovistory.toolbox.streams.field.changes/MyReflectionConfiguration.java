package org.geovistory.toolbox.streams.field.changes;

import io.quarkus.runtime.annotations.RegisterForReflection;
import org.geovistory.toolbox.streams.avro.FieldChangeJoin;
import org.geovistory.toolbox.streams.avro.FieldChangeKey;
import org.geovistory.toolbox.streams.avro.FieldChangeValue;

@RegisterForReflection(targets = {
        dev.information.statement.Key.class,
        dev.information.statement.Value.class,
        dev.projects.info_proj_rel.Key.class,
        dev.projects.info_proj_rel.Value.class,
        FieldChangeJoin.class,
        FieldChangeKey.class,
        FieldChangeValue.class
})
public class MyReflectionConfiguration {
}