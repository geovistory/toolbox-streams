package org.geovistory.toolbox.streams.lib.jsonmodels;

import com.fasterxml.jackson.annotation.JsonIncludeProperties;

@JsonIncludeProperties({"toolbox", "dataApi", "website"})
public class CommunityVisibility {
    public boolean toolbox;
    public boolean dataApi;
    public boolean website;
}
