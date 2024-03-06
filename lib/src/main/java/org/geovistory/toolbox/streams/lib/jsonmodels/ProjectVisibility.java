package org.geovistory.toolbox.streams.lib.jsonmodels;

import com.fasterxml.jackson.annotation.JsonIncludeProperties;

@JsonIncludeProperties({"dataApi", "website"})
public class ProjectVisibility {
    public boolean dataApi;
    public boolean website;
}
