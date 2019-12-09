/*
 * Copyright (c) 2010-2019 Evolveum and contributors
 *
 * This work is dual-licensed under the Apache License 2.0
 * and European Union Public License. See LICENSE file for details.
 */
package com.evolveum.polygon.connector.kafka;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.commons.lang3.StringUtils;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.objects.Attribute;
import org.identityconnectors.framework.common.objects.Name;
import org.identityconnectors.framework.common.objects.Uid;

import java.util.List;
import java.util.Set;

/**
 * @author skublik
 */

public class Subrecord extends Record implements SpecificRecord {

    private String superName;

    public Subrecord(String superName, KafkaConfiguration configuration, Schema schema, Set<Attribute> attributes) {
        super(configuration, schema, attributes);
        this.superName = superName;
    }

    protected String getAttributeName(Schema.Field field) {

        return superName + "." + field.name();
    }

}
