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

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.StreamSupport;

import org.apache.commons.lang3.Validate;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.exceptions.ConnectorIOException;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.OperationOptions;
import org.identityconnectors.framework.common.objects.Schema;
import org.identityconnectors.framework.common.objects.SyncResultsHandler;
import org.identityconnectors.framework.common.objects.SyncToken;
import org.identityconnectors.framework.spi.Configuration;
import org.identityconnectors.framework.spi.Connector;
import org.identityconnectors.framework.spi.ConnectorClass;
import org.identityconnectors.framework.spi.operations.SchemaOp;
import org.identityconnectors.framework.spi.operations.SyncOp;
import org.identityconnectors.framework.spi.operations.TestOp;

import com.evolveum.polygon.connector.kafka.CertificateRenewal.LifeState;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;

/**
 * @author skublik
 */
@ConnectorClass(displayNameKey = "connector.kafka.display", configurationClass = KafkaConfiguration.class)
public class KafkaConnector implements TestOp, SchemaOp, Connector, SyncOp{

	private static final Log LOGGER = Log.getLog(KafkaConnector.class);

	protected KafkaConfiguration configuration;

	private Schema schema = null;
	
	private KafkaConsumer<Object, Object> consumer;
	
	private SchemaRegistryClient clientSchemaRegistry;
	
	@Override
	public void test() {
		try {
			SchemaVersionInfo versionInfo = KafkaConnectorUtils.getSchemaVersionInfo(configuration, clientSchemaRegistry);
		} catch (SchemaNotFoundException e) {
			StringBuilder sb = new StringBuilder();
			sb.append("Schema with name ").append(configuration.getNameOfSchema())
			.append(" and type ").append(configuration.getVersionOfSchema()).append(" not found");
			LOGGER.error(sb.toString(), e);
		}
		
		Properties properties = KafkaConnectorUtils.getConsumerProperties(configuration);
		if(properties.contains(ConsumerConfig.MAX_POLL_RECORDS_CONFIG)) {
	    	properties.remove(ConsumerConfig.MAX_POLL_RECORDS_CONFIG);
	    }
		properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
		properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 1000);
		LOGGER.ok("Properties for consumerin test: {0}", properties.toString());
		KafkaConsumer consumer =  new KafkaConsumer<Object, Object>(properties);
		String duration = configuration.getConsumerDurationIfFail() == null ? "PT1M" : configuration.getConsumerDurationIfFail();
		List<TopicPartition> partitions = KafkaConnectorUtils.getPatritions(configuration);
		LOGGER.ok("Used partitions: {0}", partitions);
		consumer.assign(partitions);
		ConsumerRecords<Object, Object> records = consumer.poll(Duration.parse(duration).toMillis());
		consumer.commitSync();
		consumer.close();
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
			LifeState certState = certRenewal.isTrustCertificateExpiredOrNotExist();
			if (!privateKeyState.equals(LifeState.VALID) || !certState.equals(LifeState.VALID)) {
				try {
					certRenewal.renewalPrivateKeyAndCert(privateKeyState, certState);
				} catch (Exception e) {
					LOGGER.error(e, "Something wrong happened, couldn't renewal private key ");
				}
			}
		} else {
			LOGGER.info("Couldn't renewal private key and certificate");
		}
		
		this.consumer = initializeConsumer();
		this.clientSchemaRegistry=initializeSchemaRegistryClient();
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
}
