package org.geovistory.toolbox.streams.lib;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.geovistory.toolbox.streams.avro.ProjectPropertyValue;

import java.util.ArrayList;
import java.util.List;

public class ListSerdes {

    AvroSerdes avroSerdes = new AvroSerdes();

    public Serde<List<Integer>> IntegerList() {
        return Serdes.ListSerde(IntegerArrayList.class, Serdes.Integer());
    }

    public Serde<List<dev.data_for_history.api_property.Value>> DfhApiPropertyValueList() {
        return Serdes.ListSerde(DfhApiPropertyValueArrayList.class, avroSerdes.DfhApiPropertyValue());
    }

    public Serde<List<ProjectPropertyValue>> ProjectPropertyValueList() {
        return Serdes.ListSerde(ProjectPropertyValueArrayList.class, avroSerdes.ProjectPropertyValue());
    }

    public static class IntegerArrayList extends ArrayList<Integer> {
    }

    public static class DfhApiPropertyValueArrayList extends ArrayList<dev.data_for_history.api_property.Value> {
    }

    public static class ProjectPropertyValueArrayList extends ArrayList<ProjectPropertyValue> {
    }
}
