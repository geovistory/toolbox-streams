package org.geovistory.toolbox.streams.lib.jsonmodels;

import com.fasterxml.jackson.annotation.JsonIncludeProperties;

import java.util.ArrayList;

@JsonIncludeProperties({"requiredOntomeProfiles"})
public class SysConfigOntomeProfiles {
    public ArrayList<Integer> requiredOntomeProfiles;
}
