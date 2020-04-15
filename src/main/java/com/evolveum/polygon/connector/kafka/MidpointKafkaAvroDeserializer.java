/*
 * Copyright (c) 2010-2019 Evolveum and contributors
 *
 * This work is dual-licensed under the Apache License 2.0
 * and European Union Public License. See LICENSE file for details.
 */
package com.evolveum.polygon.connector.kafka;

import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.client.ISchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.exceptions.RegistryException;
import com.hortonworks.registries.schemaregistry.serde.SerDesException;
import com.hortonworks.registries.schemaregistry.serdes.Utils;
import com.hortonworks.registries.schemaregistry.serdes.avro.AvroSnapshotDeserializer;
import com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroDeserializer;
import org.identityconnectors.common.logging.Log;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Collections;
import java.util.Map;

/**
 * @author skublik
 */

public class MidpointKafkaAvroDeserializer extends KafkaAvroDeserializer {

    private static final Log LOGGER = Log.getLog(MidpointKafkaAvroDeserializer.class);

    private Map<String, Integer> readerVersions;

    private final AvroSnapshotDeserializer avroSnapshotDeserializer;
    private String nameOfSchema;
    private Integer versionOfSchema;

    public MidpointKafkaAvroDeserializer() {
        super();
        avroSnapshotDeserializer = getNewAvroSnapshotDeserializer();
    }

    public MidpointKafkaAvroDeserializer(ISchemaRegistryClient schemaRegistryClient) {
        super(schemaRegistryClient);
        avroSnapshotDeserializer = new AvroSnapshotDeserializer(schemaRegistryClient);
    }

    private AvroSnapshotDeserializer getNewAvroSnapshotDeserializer(){
        return new AvroSnapshotDeserializer() {
            @Override
            public Object deserialize(InputStream input, Integer readerSchemaVersion) throws SerDesException {
                ensureInitialized();

                // it can be enhanced to have respective protocol handlers for different versions
                byte protocolId = retrieveProtocolId(input);
                SchemaIdVersion schemaIdVersion = retrieveSchemaIdVersion(protocolId, input);
                SchemaVersionInfo schemaVersionInfo;
                SchemaMetadata schemaMetadata;
                try {
                    schemaVersionInfo = schemaRegistryClient.getSchemaVersionInfo(schemaIdVersion);
                    schemaMetadata = schemaRegistryClient.getSchemaMetadataInfo(schemaVersionInfo.getName()).getSchemaMetadata();

                    if (!schemaVersionInfo.getName().equals(nameOfSchema) || schemaVersionInfo.getVersion() != versionOfSchema) {
                        String actualVersion = schemaVersionInfo.getVersion() == null ? null : String.valueOf(schemaVersionInfo.getVersion());
                        String expectedVersion = versionOfSchema == null ? null : String.valueOf(versionOfSchema);
                        throw new IllegalStateException("Unexpected schema from record {Name: " + schemaVersionInfo.getName()
                        + ", Version: " + actualVersion + "}, expected {Name: " + nameOfSchema + ", Version: " + expectedVersion);
                    }
                } catch (Exception e) {
                    throw new RegistryException(e);
                }
                return doDeserialize(input, protocolId, schemaMetadata, schemaVersionInfo.getVersion(), readerSchemaVersion);
            }

            private void ensureInitialized() {
                if (!initialized) {
                    throw new IllegalStateException("init should be invoked before invoking deserialize operation");
                }

                if (closed) {
                    throw new IllegalStateException("This deserializer is already closed");
                }
            }
        };
    }

    @Override
    @SuppressWarnings("unchecked")
    public void configure(Map<String, ?> configs, boolean isKey) {
        super.configure(configs, isKey);
        Map<String, Integer> versions = (Map<String, Integer>) ((Map<String, Object>) configs).get(READER_VERSIONS);
        readerVersions = versions != null ? versions : Collections.emptyMap();
        nameOfSchema = (String) configs.get(KafkaConnectorUtils.SCHEMA_REGISTRY_SCHEMA_NAME);
        versionOfSchema = (Integer) configs.get(KafkaConnectorUtils.SCHEMA_REGISTRY_SCHEMA_VERSION);
        avroSnapshotDeserializer.init(configs);
    }

    @Override
    public Object deserialize(String topic, byte[] data) {
        return avroSnapshotDeserializer.deserialize(new ByteArrayInputStream(data), readerVersions.get(topic));
    }

    @Override
    public void close() {
        try {
            Utils.closeAll(avroSnapshotDeserializer);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        super.close();
    }
}
