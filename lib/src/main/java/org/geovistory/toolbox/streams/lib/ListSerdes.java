package org.geovistory.toolbox.streams.lib;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.geovistory.toolbox.streams.avro.ProjectPropertyValue;

import java.util.ArrayList;
import java.util.List;

public class ListSerdes {

    ConfluentAvroSerdes avroSerdes = new ConfluentAvroSerdes();

    public Serde<List<Integer>> IntegerList() {
        return Serdes.ListSerde(IntegerArrayList.class, Serdes.Integer());
    }

    public Serde<List<ProjectPropertyValue>> ProjectPropertyValueList() {
        return Serdes.ListSerde(ProjectPropertyValueArrayList.class, avroSerdes.ProjectPropertyValue());
    }

    public static class IntegerArrayList extends ArrayList<Integer> {
    }

    public static class ProjectPropertyValueArrayList extends ArrayList<ProjectPropertyValue> {
    }

}
