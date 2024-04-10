package org.geovistory.toolbox.streams.entity.label3.testlib;

import org.apache.kafka.streams.KeyValue;
import org.geovistory.toolbox.streams.avro.EntityLabelConfig;
import org.geovistory.toolbox.streams.avro.EntityLabelConfigPart;
import org.geovistory.toolbox.streams.avro.EntityLabelConfigPartField;
import ts.projects.entity_label_config.Key;
import ts.projects.entity_label_config.Value;

import java.util.ArrayList;


public class MockConfig {

    public static KeyValue<Key, Value> createLabelConfig(int id, int projectId, int classId, String deleted, EntityLabelConfigPartField[] parts) {

        var labelparts = new ArrayList<EntityLabelConfigPart>();
        var i = 1;
        for (var item : parts) {
            labelparts.add(EntityLabelConfigPart.newBuilder().setOrdNum(i).setField(item).build());
            i++;
        }

        var k = Key.newBuilder().setPkEntity(id).build();
        var v = Value.newBuilder()
                .setFkProject(projectId)
                .setFkClass(classId)
                .setConfig(
                        EntityLabelConfig.newBuilder().setLabelParts(labelparts).build().toString()
                )
                .setDeleted$1(deleted)
                .build();

        return KeyValue.pair(k, v);
    }


}
