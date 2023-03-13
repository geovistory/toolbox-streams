package org.geovistory.toolbox.streams.lib;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonStringifier {


    /**
     * Get a Jackson ObjectMapper able to stringify Avro class instances
     * that drops keys with null values.
     * e.g.: {"a": "b", "c": null} -> {"a": "b"}
     * @return json string without keys with null values
     */
    public static ObjectMapper getMapperIgnoringNulls() {
        ObjectMapper mapper = new ObjectMapper();
        // configure ObjectMapper to exclude null fields whiel serializing
        // mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.addMixIn(
                org.apache.avro.specific.SpecificRecord.class, // Interface implemented by all generated Avro-Classes
                JacksonIgnoreAvroPropertiesMixIn.class);
        return mapper;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public abstract static class JacksonIgnoreAvroPropertiesMixIn {

        @JsonIgnore
        public abstract org.apache.avro.Schema getSchema();

        @JsonIgnore
        public abstract org.apache.avro.specific.SpecificData getSpecificData();
    }
}
