/**
 * Copyright (c) 2010-2019 Evolveum
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.evolveum.polygon.connector.kafka;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.Validate;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.exceptions.InvalidAttributeValueException;
import org.identityconnectors.framework.common.objects.*;
import org.identityconnectors.framework.spi.Configuration;
import org.identityconnectors.framework.spi.Connector;
import org.identityconnectors.framework.spi.ConnectorClass;
import org.identityconnectors.framework.spi.operations.*;

import com.evolveum.polygon.connector.kafka.CertificateRenewal.LifeState;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;

/**
 * @author skublik
 */
@ConnectorClass(displayNameKey = "connector.kafka.display", configurationClass = KafkaConfiguration.class)
public class KafkaConnector implements TestOp, SchemaOp, Connector, SyncOp, CreateOp, UpdateDeltaOp, DeleteOp {

	private static final Log LOGGER = Log.getLog(KafkaConnector.class);

	protected KafkaConfiguration configuration;

	private Schema schema = null;
	
	private KafkaConsumer<Object, Object> consumer;
	private KafkaProducer<String, Record> producer;
	
	private SchemaRegistryClient clientSchemaRegistry;
	
	@Override
	public void test() {
		if (this.configuration.isConsumer()) {
			try {
				SchemaVersionInfo versionInfo = KafkaConnectorUtils.getSchemaVersionInfo(configuration, clientSchemaRegistry);
			} catch (SchemaNotFoundException e) {
				StringBuilder sb = new StringBuilder();
				sb.append("Schema with name ").append(configuration.getNameOfSchema())
						.append(" and type ").append(configuration.getConsumerVersionOfSchema()).append(" not found");
				LOGGER.error(sb.toString(), e);
			}

			Properties properties = KafkaConnectorUtils.getConsumerProperties(configuration);
			if (properties.contains(ConsumerConfig.MAX_POLL_RECORDS_CONFIG)) {
				properties.remove(ConsumerConfig.MAX_POLL_RECORDS_CONFIG);
			}
			properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
			properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 1000);
			LOGGER.ok("Properties for consumerin test: {0}", properties.toString());
			KafkaConsumer consumer = new KafkaConsumer<Object, Object>(properties);
			Integer duration = configuration.getConsumerDurationIfFail() == null ? 1 : configuration.getConsumerDurationIfFail();
			List<TopicPartition> partitions = KafkaConnectorUtils.getPatritions(configuration);
			LOGGER.ok("Used partitions: {0}", partitions);
			consumer.assign(partitions);
			ConsumerRecords<Object, Object> records = consumer.poll(TimeUnit.MINUTES.toMillis(duration));
			consumer.commitSync();
			consumer.close();
		}
		if (this.configuration.isProducer()){
			this.producer.partitionsFor(configuration.getProducerNameOfTopic());
		}
	}

	@Override
	public void init(Configuration configuration) {
		LOGGER.info("Initialize");
		
		KafkaConfiguration kafkaConfig = (KafkaConfiguration) configuration;
		this.configuration = kafkaConfig;
		this.configuration.validate();
		
		CertificateRenewal certRenewal = null;
		if (kafkaConfig.isValidConfigForRenewal(LOGGER)) {
			certRenewal = new CertificateRenewal(kafkaConfig);
			LifeState privateKeyState = certRenewal.isPrivateKeyExpiredOrNotExist();
			LOGGER.ok("Life state for private key is {0}", privateKeyState);
			LifeState certState = certRenewal.isTrustCertificateExpiredOrNotExist();
			LOGGER.ok("Life state for cert is {0}", certState);
			if (!privateKeyState.equals(LifeState.VALID) || !certState.equals(LifeState.VALID)) {
				if (!certRenewal.renewalPrivateKeyAndCert(privateKeyState, certState)) {
					throw new ConnectorException("Couldn't renewal private key and certificate");
				}
			}
		} else {
			LOGGER.info("Missing some configuration properties for renewal private key and certificate");
		}

		if (((KafkaConfiguration) configuration).isConsumer()) {
			this.consumer = initializeConsumer();
		}
		if (((KafkaConfiguration) configuration).isProducer()){
			this.producer = initializeProducer();
		}
		this.clientSchemaRegistry=initializeSchemaRegistryClient();
	}

	private KafkaProducer<String, Record> initializeProducer() {
		Properties properties = KafkaConnectorUtils.getProducerProperties(configuration);
		LOGGER.ok("Properties for producer: {0}", properties.toString());
		return new KafkaProducer<String, Record>(properties);
	}

	public SchemaRegistryClient initializeSchemaRegistryClient() {
		Map<String, Object> properties = KafkaConnectorUtils.getSchemaRegistryConfigProperties(configuration);
		SchemaRegistryClient client =  new SchemaRegistryClient(properties);
	    return client;
	}
	
	public KafkaConsumer<Object, Object> initializeConsumer() {
		Properties properties = KafkaConnectorUtils.getConsumerProperties(configuration);
		LOGGER.ok("Properties for consumer: {0}", properties.toString());
	    return new KafkaConsumer<Object, Object>(properties);
	}

	@Override
	public void dispose() {
		LOGGER.info("Configuration cleanup");
		configuration = null;
		if(this.consumer != null) {
			this.consumer.close();
			this.consumer = null;
		}
		if(this.clientSchemaRegistry != null) {
			this.clientSchemaRegistry.close();
			this.clientSchemaRegistry = null;
		}
	}
	
	@Override
	public Schema schema() {
		if (this.schema == null) {
			SchemaGenerator generateSchema = new SchemaGenerator(configuration, clientSchemaRegistry);
			this.schema = generateSchema.generateSchema();
		}
		return this.schema;
	}

	@Override
	public Configuration getConfiguration() {
		return configuration;
	}
	
	@Override
	public SyncToken getLatestSyncToken(ObjectClass objectClass) {
		Validate.notNull(objectClass, "Attribute of type ObjectClass not provided.");
		Synchronizer exQuery = new Synchronizer(configuration, consumer);
		return exQuery.getLatestSyncToken();
	}

	@Override
	public void sync(ObjectClass objectClass, SyncToken token, SyncResultsHandler handler, OperationOptions options) {
		if (!configuration.isConsumer()) {
			throw new UnsupportedOperationException("This operation is unsupported, if you want use this connector as " +
					"kafka consumer please set 'CONSUMER' or 'CONSUMER_AND_PRODUCER' for configuration property useOfConnector.");
		}

		Validate.notNull(objectClass, "Attribute of type ObjectClass not provided.");
		Validate.notNull(handler, "Attribute of type ResultsHandler not provided.");
		Validate.notNull(options, "Attribute of type OperationOptions not provided.");
		
		LOGGER.info("sync on {0}, token: {1}, options: {2}", objectClass, token, options);

//		if (objectClass.is(configuration.getNameOfSchema())) {
			Synchronizer synchronizer = new Synchronizer(configuration, consumer);
			synchronizer.synchronize(objectClass, token, handler, options);
//		} else {
//			LOGGER.error("Attribute of type ObjectClass is diferent as in configuration.");
//			throw new UnsupportedOperationException("Attribute of type ObjectClass is diferent as in configuration.");
//		}
	}

	@Override
	public Uid create(ObjectClass objectClass, Set<Attribute> attributes, OperationOptions options) {
		if (!configuration.isProducer()) {
			throw new UnsupportedOperationException("This operation is unsupported, if you want use this connector as " +
					"kafka producer please set 'PRODUCER' or 'CONSUMER_AND_PRODUCER' for configuration property useOfConnector.");
		}

		if (objectClass == null) {
			LOGGER.error("Attribute of type ObjectClass not provided.");
			throw new InvalidAttributeValueException("Attribute of type ObjectClass not provided.");
		}
		if (attributes == null) {
			LOGGER.error("Attribute of type Set<Attribute> not provided.");
			throw new InvalidAttributeValueException("Attribute of type Set<Attribute> not provided.");
		}

		if (options == null) {
			LOGGER.error("Parameter of type OperationOptions not provided.");
			throw new InvalidAttributeValueException("Parameter of type OperationOptions not provided.");
		}

		LOGGER.info("create on {0}, attributes: {1}, options: {2}", objectClass, attributes, options);

		ProducerOperations crateOrUpdateOp = new ProducerOperations(configuration, producer);
		Uid uid = null;
		try {
			uid = crateOrUpdateOp.createOrUpdate(attributes);
		} catch (IOException e) {
			LOGGER.error(e, "Couldn't open file " + configuration.getProducerPathToFileContainingSchema());
		}
		return uid;
	}

	@Override
	public Set<AttributeDelta> updateDelta(ObjectClass objectClass, Uid uid, Set<AttributeDelta> attrsDelta, OperationOptions options) {
		if (!configuration.isProducer()) {
			throw new UnsupportedOperationException("This operation is unsupported, if you want use this connector as " +
					"kafka producer please set 'PRODUCER' or 'CONSUMER_AND_PRODUCER' for configuration property useOfConnector.");
		}

		if (objectClass == null) {
			LOGGER.error("Parameter of type ObjectClass not provided.");
			throw new InvalidAttributeValueException("Parameter of type ObjectClass not provided.");
		}

		if (uid.getUidValue() == null || uid.getUidValue().isEmpty()) {
			LOGGER.error("Parameter of type Uid not provided or is empty.");
			throw new InvalidAttributeValueException("Parameter of type Uid not provided or is empty.");
		}

		if (attrsDelta == null) {
			LOGGER.error("Parameter of type Set<AttributeDelta> not provided.");
			throw new InvalidAttributeValueException("Parameter of type Set<AttributeDelta> not provided.");
		}

		if (options == null) {
			LOGGER.error("Parameter of type OperationOptions not provided.");
			throw new InvalidAttributeValueException("Parameter of type OperationOptions not provided.");
		}

		LOGGER.info("updateDelta on {0}, uid: {1}, attrDelta: {2}, options: {3}", objectClass, uid.getValue(), attrsDelta, options);

		Set<Attribute> attributeReplace = new HashSet<Attribute>();
		for (AttributeDelta attrDelta : attrsDelta){
			List<Object> replaceValue = attrDelta.getValuesToReplace();
				attributeReplace.add(AttributeBuilder.build(attrDelta.getName(), replaceValue));
		}
		attributeReplace.add(uid);

		ProducerOperations producerOp = new ProducerOperations(configuration, producer);
		try {
			producerOp.createOrUpdate(attributeReplace);
		} catch (IOException e) {
			LOGGER.error(e, "Couldn't open file " + configuration.getProducerPathToFileContainingSchema());
		}
		return new HashSet<AttributeDelta>();
	}

	@Override
	public void delete(ObjectClass objectClass, Uid uid, OperationOptions operationOptions) {
		if (!configuration.isProducer()) {
			throw new UnsupportedOperationException("This operation is unsupported, if you want use this connector as " +
					"kafka producer please set 'PRODUCER' or 'CONSUMER_AND_PRODUCER' for configuration property useOfConnector.");
		}

		if (objectClass == null) {
			LOGGER.error("Attribute of type ObjectClass not provided.");
			throw new InvalidAttributeValueException("Attribute of type ObjectClass not provided.");
		}
		if (uid.getUidValue() == null || uid.getUidValue().isEmpty()) {
			LOGGER.error("Attribute of type Uid not provided or is empty.");
			throw new InvalidAttributeValueException("Attribute of type Uid not provided or is empty.");
		}

		LOGGER.info("Delete on {0}, uid: {1}, options: {2}", objectClass, uid.getValue(), operationOptions);

		ProducerOperations producerOp = new ProducerOperations(configuration, producer);

		producerOp.executeDeleteOperation(uid);
	}
}
