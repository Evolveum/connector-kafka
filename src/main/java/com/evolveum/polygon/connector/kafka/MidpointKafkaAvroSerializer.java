/*
 * Copyright (c) 2010-2019 Evolveum and contributors
 *
 * This work is dual-licensed under the Apache License 2.0
 * and European Union Public License. See LICENSE file for details.
 */
package com.evolveum.polygon.connector.kafka;

import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaVersion;
import com.hortonworks.registries.schemaregistry.client.ISchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.errors.IncompatibleSchemaException;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaException;
import com.hortonworks.registries.schemaregistry.errors.SchemaBranchNotFoundException;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import com.hortonworks.registries.schemaregistry.exceptions.RegistryException;
import com.hortonworks.registries.schemaregistry.serde.SerDesException;
import com.hortonworks.registries.schemaregistry.serdes.Utils;
import com.hortonworks.registries.schemaregistry.serdes.avro.AvroSnapshotSerializer;
import com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroSerializer;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * @author skublik
 */

public class MidpointKafkaAvroSerializer implements Serializer<Object> {

  private final KafkaAvroSerializer serializer;
//  private final MidpointAvroSnapshotSerializer avroSnapshotSerializer;

  public MidpointKafkaAvroSerializer() {
    this.serializer = new KafkaAvroSerializer();
//    this.avroSnapshotSerializer = new MidpointAvroSnapshotSerializer();
  }

  public MidpointKafkaAvroSerializer(ISchemaRegistryClient schemaRegistryClient) {
    this.serializer = new KafkaAvroSerializer(schemaRegistryClient);
//    this.avroSnapshotSerializer = new MidpointAvroSnapshotSerializer(schemaRegistryClient);
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    serializer.configure(configs, isKey);
//    avroSnapshotSerializer.init(configs);
  }

  @Override
  public byte[] serialize(String topic, Object data) {
    if (data == null) {
      return null;
    } else {
      String nameOfSchema = topic;
      if (data instanceof Record) {
        nameOfSchema = ((Record) data).getConfiguration().getNameOfSchema();
      }
      return serializer.serialize(nameOfSchema, data);
//      return avroSnapshotSerializer.customSerialize((Record) data);
    }
  }

//  @Override
//  public void close() {
//    try {
//      Utils.closeAll(avroSnapshotSerializer);
//    } catch (Exception e) {
//      e.printStackTrace();
//    }
//  }

  @Override
  public void close() {
    serializer.close();
  }

//  private class MidpointAvroSnapshotSerializer extends AvroSnapshotSerializer {
//
//    public MidpointAvroSnapshotSerializer() {
//      super();
//    }
//
//    public MidpointAvroSnapshotSerializer(ISchemaRegistryClient schemaRegistryClient) {
//      super(schemaRegistryClient);
//    }
//
//    public byte[] customSerialize(Record input) throws SerDesException {
//      this.ensureInitialized();
//      String schema = this.getSchemaText(input);
//      try {
//        SchemaIdVersion schemaIdVersion = KafkaConnectorUtils.getSchemaIdVersion(input.getConfiguration(), this.schemaRegistryClient);
//        return this.doSerialize(input, schemaIdVersion);
//      } catch (SchemaBranchNotFoundException | SchemaNotFoundException var5) {
//        throw new RegistryException(var5);
//      }
//    }
//
//    private void ensureInitialized() {
//      if (!this.initialized) {
//        throw new IllegalStateException("init should be invoked before invoking serialize operation");
//      } else if (this.closed) {
//        throw new IllegalStateException("This serializer is already closed");
//      }
//    }
//
//  }

}
