package org.geovistory.toolbox.streams.project.items.lib;

import org.geovistory.toolbox.streams.avro.EdgeVisibilityValue;

public interface ExtractVisiblity {
    boolean get(EdgeVisibilityValue v);
}