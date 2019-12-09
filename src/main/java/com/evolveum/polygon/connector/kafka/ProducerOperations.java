/*
 * Copyright (c) 2010-2019 Evolveum and contributors
 *
 * This work is dual-licensed under the Apache License 2.0
 * and European Union Public License. See LICENSE file for details.
 */
package com.evolveum.polygon.connector.kafka;

import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.objects.Attribute;
import org.identityconnectors.framework.common.objects.Uid;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * @author skublik
 */

public class ProducerOperations {

    private static final Log LOGGER = Log.getLog(ProducerOperations.class);

    private KafkaConfiguration configuration;
    private KafkaProducer<String, Record> producer;
    private final Callback callback = new PublishCallback();

    public ProducerOperations(KafkaConfiguration configuration, KafkaProducer<String, Record> producer){
        this.configuration = configuration;
        this.producer = producer;
    }

    public Uid createOrUpdate(Set<Attribute> attributes) throws IOException {
        Schema schema = KafkaConnectorUtils.getSchemaForProducerFromFile(configuration);
        Attribute uniqueAttr = KafkaConnectorUtils.getUid(attributes);
        if (uniqueAttr == null && KafkaConnectorUtils.isUniqueAndNameAttributeEqual(configuration)) {
            uniqueAttr = KafkaConnectorUtils.getName(attributes);
        }
        if (uniqueAttr == null) {
            uniqueAttr = KafkaConnectorUtils.getAttr(attributes, configuration.getUniqueAttribute());
        }
        if (uniqueAttr == null) {
            throw new ConnectorException("Couldn't find unique attribute " + configuration.getUniqueAttribute() + " in contains attributes " + attributes);
        }

        if (!(uniqueAttr.getValue().get(0) instanceof String)) {
            throw new ConnectorException("Type of uniqueAttribute have to be string.");
        }
//        AccountKey key = new AccountKey(configuration, schema, uniqueAttr);
        Record account = new Record(configuration, schema, attributes);

        sendRecord(uniqueAttr, account);

        return new Uid((String) uniqueAttr.getValue().get(0));

    }

    public void executeDeleteOperation(Uid uid) {
        sendRecord(uid, null);
    }

    private void sendRecord(Attribute uniqueAttr, Record account) {
        String key = (String) uniqueAttr.getValue().get(0);
        ProducerRecord<String, Record> record = new ProducerRecord<String, Record>(configuration.getProducerNameOfTopic(), key, account);

        try {
            producer.send(record, callback).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            LOGGER.error("Failed to publish the record.", e);
        }
    }
}
