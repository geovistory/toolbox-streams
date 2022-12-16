package org.geovistory.toolbox.streams.lib;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import java.util.ArrayList;
import java.util.List;

public class ListSerdes {

    public Serde<List<Integer>> IntegerList() {
        return Serdes.ListSerde(IntegerArrayList.class, Serdes.Integer());
    }


    public static class IntegerArrayList extends ArrayList<Integer> {
    }

}
