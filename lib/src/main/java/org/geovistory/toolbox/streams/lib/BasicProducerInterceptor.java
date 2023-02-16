package org.geovistory.toolbox.streams.lib;


import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Map;

public class BasicProducerInterceptor implements ProducerInterceptor<Object, Object> {


    private static final Logger LOG = LoggerFactory.getLogger(BasicProducerInterceptor.class);

    @Override
    public ProducerRecord<Object, Object> onSend(ProducerRecord<Object, Object> record) {

        LOG.info(Instant.now() + " ProducerRecord being sent out {} ", record);

        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (exception != null) {
            LOG.warn(Instant.now() + " Exception encountered producing record {}", exception);
        } else {
            LOG.info(Instant.now() + " record has been acknowledged {} ", metadata);
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}