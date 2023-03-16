package org.geovistory.toolbox.streams.fulltext.processors;

import org.geovistory.toolbox.streams.avro.EntityFieldTextMapWithConfigValue;
import org.geovistory.toolbox.streams.avro.EntityLabelConfigPart;
import org.geovistory.toolbox.streams.avro.FieldLabelWithTopLabelsValue;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Objects;
import java.util.TreeMap;

public class FullTextFactory {
    /**
     * creates the fulltext for an entity
     *
     * @param v a ProjectEntityTopStatementsWithConfigValue
     * @return Fulltext string.
     */
    public static String createFulltext(EntityFieldTextMapWithConfigValue v) {
        var strings = new ArrayList<String>();

        var labelConfig = v.getLabelConfig();
        var fieldsMap = new TreeMap<>(v.getFields());

        // first process fields from label config
        if (labelConfig != null) {
            labelConfig.getLabelParts().stream()
                    .sorted(Comparator.comparingInt(EntityLabelConfigPart::getOrdNum))
                    .forEach(entityLabelConfigPart -> {
                        var f = entityLabelConfigPart.getField();
                        var k = getFieldKey(f.getIsOutgoing(), f.getFkProperty());
                        var field = fieldsMap.get(k);

                        var s = createFieldText(field);
                        if (s != null && !s.equals("")) strings.add(s);

                        // remove key from map
                        fieldsMap.remove(k);

                    });
        }

        // then process rest of fields
        fieldsMap.forEach((key, value) -> {
            var s = createFieldText(value);
            if (s != null && !s.equals("")) strings.add(s);
        });

        return String.join(".\n", strings) + (strings.size() > 0 ? "." : "");
    }


    private static String createFieldText(FieldLabelWithTopLabelsValue field) {
        if (field == null) return null;
        var fieldStrings = new ArrayList<String>();
        field.getTargetLabels().forEach(s -> {
            if (s != null && !s.equals("")) fieldStrings.add(s);
        });

        if (fieldStrings.size() > 0) {
            var propertyLabel = field.getPropertyLabel();
            var targetLabels = String.join(", ", fieldStrings);
            if (propertyLabel != null && !Objects.equals(propertyLabel, "")) {
                return propertyLabel + ": " + targetLabels;
            }
            return targetLabels;
        }
        return null;
    }


    public static String getFieldKey(boolean isOutgoing, int propertyId) {
        return propertyId + "_" + (isOutgoing ? "o" : "i");
    }
}
