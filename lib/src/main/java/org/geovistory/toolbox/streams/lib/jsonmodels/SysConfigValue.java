package org.geovistory.toolbox.streams.lib.jsonmodels;

import com.fasterxml.jackson.annotation.JsonIncludeProperties;

@JsonIncludeProperties({"ontome"})
public class SysConfigValue {
    public SysConfigOntomeProfiles ontome;

}
