/*
 * Copyright (c) 2010-2019 Evolveum and contributors
 *
 * This work is dual-licensed under the Apache License 2.0
 * and European Union Public License. See LICENSE file for details.
 */
package com.evolveum.polygon.connector.kafka;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.specific.SpecificRecord;
import org.apache.commons.lang3.Validate;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.objects.Attribute;
import org.identityconnectors.framework.common.objects.Uid;

import java.util.Collections;

/**
 * @author skublik
 */

public class AccountKey extends Record implements SpecificRecord {

    private final static String NAME_SUFFIX = "Key";

    private Attribute uniqueAttr;
    private KafkaConfiguration configuration;

    public AccountKey(KafkaConfiguration configuration, Schema valueSchema, Attribute uniqueAttr) {
        super(configuration, createSchemaForKey(valueSchema, uniqueAttr, configuration.getUniqueAttribute()), Collections.singleton(uniqueAttr));
        this.uniqueAttr = uniqueAttr;
        this.configuration = configuration;
    }

    private static Schema createSchemaForKey(Schema valueSchema, Attribute uniqueAttr, String uniqueAttrName) {
        Validate.notNull(uniqueAttr);
        Validate.notNull(valueSchema);
        Validate.notBlank(uniqueAttrName);

        if (uniqueAttr.getValue().size() > 1) {
            throw new ConnectorException("uniqueAttribute " + uniqueAttrName + " can't be multivalue " + uniqueAttrName + " in schema");
        }

        Schema.Field uniqueField = null;
        for (Schema.Field field : valueSchema.getFields()) {
            if (field.name().equals(uniqueAttrName)){
                uniqueField = field;
            }
        }
        if (uniqueField == null) {
            throw new ConnectorException("Couldn't find uniqueAttribute " + uniqueAttrName + " in schema");
        }

        if (!Schema.Type.STRING.equals(uniqueField.schema().getType())){
            throw new ConnectorException("Type of uniqueAttribute have to be string, but is " + uniqueField.schema().getType());
        }

        Schema schema = SchemaBuilder
                .record(valueSchema.getName() + NAME_SUFFIX)
                .namespace(valueSchema.getNamespace())
                .fields()
                .name(uniqueField.name())
                .type(uniqueField.schema())
                .noDefault()
                .endRecord();
        return schema;
    }

    @Override
    public Object get(int i) {
        Schema schema = getSchema();
        String name = schema.getFields().get(i).name();
        if (uniqueAttr.getName().equals(name) || Uid.NAME.equals(uniqueAttr.getName())){
            return uniqueAttr.getValue().get(0);
        }
        return null;
    }

    public Attribute getUniqueAttr() {
        return uniqueAttr;
    }
}
