/*
 * Copyright (c) 2010-2019 Evolveum and contributors
 *
 * This work is dual-licensed under the Apache License 2.0
 * and European Union Public License. See LICENSE file for details.
 */
package com.evolveum.polygon.connector.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author skublik
 */

public class PublishCallback implements Callback {

    private static final Logger LOG = LoggerFactory.getLogger(PublishCallback.class);

    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        if (e == null) {
            LOG.debug("Record has been published successfully.");
        } else {
            LOG.warn("Failed to publish the record.", e);
        }
    }

}