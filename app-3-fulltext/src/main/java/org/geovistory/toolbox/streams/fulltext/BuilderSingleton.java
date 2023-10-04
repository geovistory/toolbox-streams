package org.geovistory.toolbox.streams.fulltext;

import org.apache.kafka.streams.StreamsBuilder;

import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class BuilderSingleton {
    public StreamsBuilder builder = new StreamsBuilder();
}
