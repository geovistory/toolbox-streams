package org.geovistory.toolbox.streams.entity.processors.lib;


import org.geovistory.toolbox.streams.avro.TimePrimitive;
import org.geovistory.toolbox.streams.avro.TopTimePrimitives;
import org.geovistory.toolbox.streams.avro.TopTimePrimitivesMap;
import org.geovistory.toolbox.streams.entity.lib.TimeSpanFactory;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class TimeSpanFactoryTest {


    @Test
    void testCreateTimeSpanMethod() {
        long expectedFirstSec = 204139785600L;
        long expectedLastSec = 204139871999L;

        var ttp = TopTimePrimitivesMap.newBuilder().build();
        var map = ttp.getMap();

        var propertyId = 71;
        var v = TopTimePrimitives.newBuilder()
                .setPropertyId(propertyId)
                .setTimePrimitives(
                        List.of(
                                TimePrimitive.newBuilder()
                                        .setJulianDay(2362729)
                                        .setDuration("1 day")
                                        .setCalendar("gregorian")
                                        .build()
                        )
                ).build();

        map.put("" + propertyId, v);

        var result = TimeSpanFactory.createTimeSpan(ttp);

        assertThat(result).isNotNull();
        assertThat(result.getTimeSpan().getP81().getDuration()).isEqualTo("1 day");
        assertThat(result.getTimeSpan().getP81a()).isNull();
        assertThat(result.getFirstSecond()).isEqualTo(expectedFirstSec);
        assertThat(result.getLastSecond()).isEqualTo(expectedLastSec);
    }

    @Test
    void testMethodWithoutTemporalData() {
        var ttp = TopTimePrimitivesMap.newBuilder().build();

        var result = TimeSpanFactory.createTimeSpan(ttp);

        assertThat(result).isNull();
    }


}
