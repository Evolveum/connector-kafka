/*
 * Copyright (c) 2010-2019 Evolveum and contributors
 *
 * This work is dual-licensed under the Apache License 2.0
 * and European Union Public License. See LICENSE file for details.
 */
package com.evolveum.polygon.connector.kafka;

import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.commons.lang3.StringUtils;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.objects.Attribute;
import org.identityconnectors.framework.common.objects.Name;
import org.identityconnectors.framework.common.objects.OperationalAttributes;
import org.identityconnectors.framework.common.objects.Uid;

import java.util.List;
import java.util.Set;

/**
 * @author skublik
 */

public class Record extends SpecificRecordBase implements SpecificRecord {

    private static final Log LOGGER = Log.getLog(KafkaConnector.class);

    private Schema schema;
    private Set<Attribute> attributes;
    private KafkaConfiguration configuration;

    public Record(KafkaConfiguration configuration, Schema schema, Set<Attribute> attributes) {
        this.configuration = configuration;
        this.schema = schema;
        this.attributes = attributes;
    }

    @Override
    public Schema getSchema() {
        return schema;
    }

    protected String getAttributeName(Schema.Field field) {
        return field.name();
    }

    @Override
    public Object get(int i) {
        Schema.Field field = schema.getFields().get(i);
        if (field == null) {
            String message = "Field with index " + i + " from producer avro schema is null";
            ConnectorException e = new ConnectorException(message);
            LOGGER.error(e, message);
            throw e;
        }
        String name = getAttributeName(field);
        if (StringUtils.isBlank(name)) {
            String message = "Name of attribute from producer avro schema is null";
            ConnectorException e = new ConnectorException(message);
            LOGGER.error(e, message);
            throw e;
        }

        Schema.Type schemaType = field.schema().getType();
        if (schemaType.equals(Schema.Type.UNION)) {
            schemaType = SchemaGenerator.getTypeForUnion(field.schema());
        }

        if (schemaType.equals(Schema.Type.RECORD)) {
            return new Subrecord(name, configuration, field.schema(), attributes);
        }
        boolean isMultivalue = schemaType.equals(Schema.Type.ARRAY);
        if (SchemaGenerator.isPrimitive(schemaType) || isMultivalue) {
            for (Attribute attr : attributes) {
                if ((name.equals(configuration.getPasswordAttribute()) && OperationalAttributes.PASSWORD_NAME.equals(attr.getName()))) {
                    GuardedString pass = (GuardedString) attr.getValue().get(0);

                    if (pass != null) {
                        final StringBuilder sbPass = new StringBuilder();
                        pass.access(new GuardedString.Accessor() {
                            @Override
                            public void access(char[] chars) {
                                sbPass.append(new String(chars));
                            }
                        });
                        return sbPass.toString();
                    }

                    return  null;
                }
                if ((name.equals(configuration.getUniqueAttribute()) && Uid.NAME.equals(attr.getName()))
                        || (name.equals(configuration.getUniqueAttribute())
                            && KafkaConnectorUtils.isUniqueAndNameAttributeEqual(configuration) && Name.NAME.equals(attr.getName()))
                        || (name.equals(configuration.getNameAttribute()) && Name.NAME.equals(attr.getName()))
                        || name.equals(attr.getName())) {
                    List<Object> values = attr.getValue();
                    if (values.isEmpty()) {
                        return  returnNotFoundedAttribute(name, field, isMultivalue);
                    } else if (!isMultivalue) {
                        return values.get(0);
                    } else {
                        return values;
                    }
                }
            }
            return  returnNotFoundedAttribute(name, field, isMultivalue);
        }
        StringBuilder sb = new StringBuilder();
        sb.append("Connector don't support 'enum','map' and 'fixed'");
        LOGGER.error(sb.toString());
        throw new ConnectorException(sb.toString());
    }

    private Object returnNotFoundedAttribute(String name, Schema.Field field, boolean isMultivalue) {
        boolean isRequired = (field.defaultVal() == null);
        if (isRequired) {
            String message = "Attribute " + name + " is required";
            ConnectorException e = new ConnectorException(message);
            LOGGER.error(e, message);
            throw e;
        }
        Object ret = field.defaultVal();
        if (ret instanceof JsonProperties.Null) {
            return null;
        }
        return ret;
    }

    @Override
    public void put(int i, Object o) {
        throw new UnsupportedOperationException();
    }

    public KafkaConfiguration getConfiguration() {
        return configuration;
    }
}
