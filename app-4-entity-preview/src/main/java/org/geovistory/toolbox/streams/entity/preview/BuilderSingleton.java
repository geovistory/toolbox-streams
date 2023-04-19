package org.geovistory.toolbox.streams.entity.preview;

import org.apache.kafka.streams.StreamsBuilder;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class BuilderSingleton {
    public StreamsBuilder builder = new StreamsBuilder();
}
