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

import org.identityconnectors.common.StringUtil;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.common.exceptions.ConfigurationException;
import org.identityconnectors.framework.spi.AbstractConfiguration;
import org.identityconnectors.framework.spi.ConfigurationProperty;
import org.identityconnectors.framework.spi.StatefulConfiguration;

/**
 * @author skublik
 */
public class KafkaConfiguration extends AbstractConfiguration implements StatefulConfiguration{

	private static final Log LOGGER = Log.getLog(KafkaConnector.class);

	private enum UseOfConnector {
		CONSUMER,
		PRODUCER,
		CONSUMER_AND_PRODUCER
	}

	private String schemaRegistryUrl;
	private String pathToMorePropertiesForSchemaRegistry;
	private String schemaRegistrySslProtocol;

	private String ssoUrlRenewal;
	private String serviceUrlRenewal;
	private String usernameRenewal;
	private GuardedString passwordRenewal;
	private String clientIdRenewal;
	private Integer intervalForCertificateRenewal;
	private String sslPrivateKeyEntryAlias;
	private GuardedString sslPrivateKeyEntryPassword;
	private String sslTrustCertificateAliasPrefix;

	private String useOfConnector;
	private String uniqueAttribute;
	private String nameAttribute;
	private String passwordAttribute;
	private String bootstrapServers;
	private String nameOfSchema;
	private String kafkaSecurityProtocol;
	private String sslKeyStoreType;
	private String sslKeyStorePath;
	private GuardedString sslKeyStorePassword;
	private String sslKeyStoreProvider;
	private GuardedString sslKeyPassword;
	private String sslKeyManagerFactoryProvider;
	private String sslKeyManagerFactoryAlgorithm;
	private String sslTrustStoreType;
	private String sslTrustStorePath;
	private GuardedString sslTrustStorePassword;
	private String sslTrustStoreProvider;
	private String sslTrustManagerFactoryProvider;
	private String sslTrustManagerFactoryAlgorithm;

	private String consumerNameOfTopic;
	private Integer consumerVersionOfSchema;
	private String consumerGroupId;
	private String consumerPartitionOfTopic;
	private Integer consumerDurationIfFail;
	private Integer consumerMaxRecords;
	private String pathToMorePropertiesForConsumer;

	private String producerPathToFileContainingSchema;
	private String producerNameOfTopic;
	private String pathToMorePropertiesForProducer;


	// schema registry properties

	@ConfigurationProperty(order = 3, displayMessageKey = "schemaRegistryUrl.display",
			helpMessageKey = "schemaRegistryUrl.help", required = true, confidential = false)
	public String getSchemaRegistryUrl() {
		return schemaRegistryUrl;
	}
	
	public void setSchemaRegistryUrl(String schemaRegistryUrl) {
		this.schemaRegistryUrl = schemaRegistryUrl;
	}

	@ConfigurationProperty(order = 9, displayMessageKey = "pathToMorePropertiesForSchemaRegistry.display",
			helpMessageKey = "pathToMorePropertiesForSchemaRegistry.help", required = false, confidential = false)
	public String getPathToMorePropertiesForSchemaRegistry() {
		return pathToMorePropertiesForSchemaRegistry;
	}
	
	public void setPathToMorePropertiesForSchemaRegistry(String pathToMorePropertiesForSchemaRegistry) {
		this.pathToMorePropertiesForSchemaRegistry = pathToMorePropertiesForSchemaRegistry;
	}
	
	@ConfigurationProperty(order = 27, displayMessageKey = "schemaRegistrySslProtocol.display",
			helpMessageKey = "schemaRegistrySslProtocol.help", required = false, confidential = false)
	public String getSchemaRegistrySslProtocol() {
		return schemaRegistrySslProtocol;
	}

	public void setSchemaRegistrySslProtocol(String sslProtocol) {
		this.schemaRegistrySslProtocol = sslProtocol;
	}


	// cert renewal properties

	@ConfigurationProperty(order = 29, displayMessageKey = "ssoUrlRenewal.display",
			helpMessageKey = "ssoUrlRenewal.help", required = false, confidential = false)
	public String getSsoUrlRenewal() {
		return ssoUrlRenewal;
	}

	public void setSsoUrlRenewal(String ssoUrlRenewal) {
		this.ssoUrlRenewal = ssoUrlRenewal;
	}

	@ConfigurationProperty(order = 30, displayMessageKey = "serviceUrlRenewal.display",
			helpMessageKey = "serviceUrlRenewal.help", required = false, confidential = false)
	public String getServiceUrlRenewal() {
		return serviceUrlRenewal;
	}

	public void setServiceUrlRenewal(String serviceUrlRenewal) {
		this.serviceUrlRenewal = serviceUrlRenewal;
	}

	@ConfigurationProperty(order = 31, displayMessageKey = "usernameRenewal.display",
			helpMessageKey = "usernameRenewal.help", required = false, confidential = false)
	public String getUsernameRenewal() {
		return usernameRenewal;
	}

	public void setUsernameRenewal(String usernameRenewal) {
		this.usernameRenewal = usernameRenewal;
	}

	@ConfigurationProperty(order = 32, displayMessageKey = "passwordRenewal.display",
			helpMessageKey = "passwordRenewal.help", required = false, confidential = false)
	public GuardedString getPasswordRenewal() {
		return passwordRenewal;
	}

	public void setPasswordRenewal(GuardedString passwordRenewal) {
		this.passwordRenewal = passwordRenewal;
	}

	@ConfigurationProperty(order = 33, displayMessageKey = "clientIdRenewal.display",
			helpMessageKey = "clientIdRenewal.help", required = false, confidential = false)
	public String getClientIdRenewal() {
		return clientIdRenewal;
	}

	public void setClientIdRenewal(String clientIdRenewal) {
		this.clientIdRenewal = clientIdRenewal;
	}

	@ConfigurationProperty(order = 34, displayMessageKey = "intervalForCertificateRenewal.display",
			helpMessageKey = "intervalForCertificateRenewal.help", required = false, confidential = false)
	public Integer getIntervalForCertificateRenewal() {
		return intervalForCertificateRenewal;
	}

	public void setIntervalForCertificateRenewal(Integer intervalForCertificateRenewal) {
		this.intervalForCertificateRenewal = intervalForCertificateRenewal;
	}

	@ConfigurationProperty(order = 35, displayMessageKey = "sslPrivateKeyEntryAlias.display",
			required = false, confidential = false)
	public String getSslPrivateKeyEntryAlias() {
		return sslPrivateKeyEntryAlias;
	}

	public void setSslPrivateKeyEntryAlias(String sslPrivateKeyEntryAlias) {
		this.sslPrivateKeyEntryAlias = sslPrivateKeyEntryAlias;
	}

	@ConfigurationProperty(order = 36, displayMessageKey = "sslPrivateKeyEntryPassword.display",
			required = false, confidential = false)
	public GuardedString getSslPrivateKeyEntryPassword() {
		return sslPrivateKeyEntryPassword;
	}

	public void setSslPrivateKeyEntryPassword(GuardedString sslPrivateKeyEntryPassword) {
		this.sslPrivateKeyEntryPassword = sslPrivateKeyEntryPassword;
	}

	@ConfigurationProperty(order = 37, displayMessageKey = "sslTrustCertificateAliasPrefix.display",
			helpMessageKey = "sslTrustCertificateAliasPrefix.help", required = false, confidential = false)
	public String getSslTrustCertificateAliasPrefix() {
		return sslTrustCertificateAliasPrefix;
	}

	public void setSslTrustCertificateAliasPrefix(String sslTrustCertificateAliasPrefix) {
		this.sslTrustCertificateAliasPrefix = sslTrustCertificateAliasPrefix;
	}


	//common properties for consumer and producer

	@ConfigurationProperty(order = 38, displayMessageKey = "useOfConnector.display",
			helpMessageKey = "useOfConnector.help", required = true, confidential = false)
	public String getUseOfConnector() {
		return useOfConnector;
	}

	public void setUseOfConnector(String useOfConnector) {
		this.useOfConnector = useOfConnector;
	}

	@ConfigurationProperty(order = 39, displayMessageKey = "uniqueAttribute.display",
			helpMessageKey = "uniqueAttribute.help", required = true, confidential = false)
	public String getUniqueAttribute() {
		return uniqueAttribute;
	}

	public void setUniqueAttribute(String uniqueAttribute) {
		this.uniqueAttribute = uniqueAttribute;
	}

	@ConfigurationProperty(order = 40, displayMessageKey = "bootstrapServers.display",
			helpMessageKey = "bootstrapServers.help", required = true, confidential = false)
	public String getBootstrapServers() {
		return bootstrapServers;
	}

	public void setBootstrapServers(String bootstrapServers) {
		this.bootstrapServers = bootstrapServers;
	}

	@ConfigurationProperty(order = 41, displayMessageKey = "nameOfSchema.display",
			helpMessageKey = "nameOfSchema.help", required = true, confidential = false)
	public String getNameOfSchema() {
		return nameOfSchema;
	}

	public void setNameOfSchema(String nameOfSchema) {
		this.nameOfSchema = nameOfSchema;
	}

	@ConfigurationProperty(order = 42, displayMessageKey = "kafkaSecurityProtocol.display",
			required = false, confidential = false)
	public String getKafkaSecurityProtocol() {
		return kafkaSecurityProtocol;
	}

	public void setKafkaSecurityProtocol(String kafkaSecurityProtocol) {
		this.kafkaSecurityProtocol = kafkaSecurityProtocol;
	}

	@ConfigurationProperty(order = 43, displayMessageKey = "passwordAttribute.display",
			helpMessageKey = "passwordAttribute.help", required = false, confidential = false)
	public String getPasswordAttribute() {
		return passwordAttribute;
	}

	public void setPasswordAttribute(String passwordAttribute) {
		this.passwordAttribute = passwordAttribute;
	}

	@ConfigurationProperty(order = 43, displayMessageKey = "nameAttribute.display",
			helpMessageKey = "nameAttribute.help", required = false, confidential = false)
	public String getNameAttribute() {
		return nameAttribute;
	}

	public void setNameAttribute(String nameAttribute) {
		this.nameAttribute = nameAttribute;
	}

	@ConfigurationProperty(order = 45, displayMessageKey = "sslKeyStoreType.display",
			helpMessageKey = "sslKeyStoreType.help", required = false, confidential = false)
	public String getSslKeyStoreType() {
		return sslKeyStoreType;
	}
	
	public void setSslKeyStoreType(String sslKeyStoreType) {
		this.sslKeyStoreType = sslKeyStoreType;
	}

	@ConfigurationProperty(order = 46, displayMessageKey = "sslKeyStorePath.display",
			helpMessageKey = "sslKeyStorePath.help", required = false, confidential = false)
	public String getSslKeyStorePath() {
		return sslKeyStorePath;
	}
	
	public void setSslKeyStorePath(String sslKeyStorePath) {
		this.sslKeyStorePath = sslKeyStorePath;
	}

	@ConfigurationProperty(order = 47, displayMessageKey = "sslKeyStorePassword.display",
			helpMessageKey = "sslKeyStorePassword.help", required = false, confidential = true)
	public GuardedString getSslKeyStorePassword() {
		return sslKeyStorePassword;
	}
	
	public void setSslKeyStorePassword(GuardedString sslKeyStorePassword) {
		this.sslKeyStorePassword = sslKeyStorePassword;
	}
	
	@ConfigurationProperty(order = 48, displayMessageKey = "sslKeyStoreProvider.display",
			helpMessageKey = "sslKeyStoreProvider.help", required = false, confidential = false)
	public String getSslKeyStoreProvider() {
		return sslKeyStoreProvider;
	}
	
	public void setSslKeyStoreProvider(String sslKeyStoreProvider) {
		this.sslKeyStoreProvider = sslKeyStoreProvider;
	}

	@ConfigurationProperty(order = 49, displayMessageKey = "sslKeyPassword.display",
			helpMessageKey = "sslKeyPassword.help", required = false, confidential = true)
	public GuardedString getSslKeyPassword() {
		return sslKeyPassword;
	}
	
	public void setSslKeyPassword(GuardedString sslKeyPassword) {
		this.sslKeyPassword = sslKeyPassword;
	}

	@ConfigurationProperty(order = 50, displayMessageKey = "sslKeyManagerFactoryProvider.display",
			helpMessageKey = "sslKeyManagerFactoryProvider.help", required = false, confidential = false)
	public String getSslKeyManagerFactoryProvider() {
		return sslKeyManagerFactoryProvider;
	}
	
	public void setSslKeyManagerFactoryProvider(String sslKeyManagerFactoryProvider) {
		this.sslKeyManagerFactoryProvider = sslKeyManagerFactoryProvider;
	}

	@ConfigurationProperty(order = 51, displayMessageKey = "sslKeyManagerFactoryAlgorithm.display",
			helpMessageKey = "sslKeyManagerFactoryAlgorithm.help", required = false, confidential = false)
	public String getSslKeyManagerFactoryAlgorithm() {
		return sslKeyManagerFactoryAlgorithm;
	}
	
	public void setSslKeyManagerFactoryAlgorithm(String sslKeyManagerFactoryAlgorithm) {
		this.sslKeyManagerFactoryAlgorithm = sslKeyManagerFactoryAlgorithm;
	}

	@ConfigurationProperty(order = 52, displayMessageKey = "sslTrustStoreType.display",
			helpMessageKey = "sslTrustStoreType.help", required = false, confidential = false)
	public String getSslTrustStoreType() {
		return sslTrustStoreType;
	}
	
	public void setSslTrustStoreType(String sslTrustStoreType) {
		this.sslTrustStoreType = sslTrustStoreType;
	}

	@ConfigurationProperty(order = 60, displayMessageKey = "sslTrustStorePath.display",
			helpMessageKey = "sslTrustStorePath.help", required = false, confidential = false)
	public String getSslTrustStorePath() {
		return sslTrustStorePath;
	}
	
	public void setSslTrustStorePath(String sslTrustStorePath) {
		this.sslTrustStorePath = sslTrustStorePath;
	}

	@ConfigurationProperty(order = 61, displayMessageKey = "sslTrustStorePassword.display",
			helpMessageKey = "sslTrustStorePassword.help", required = false, confidential = true)
	public GuardedString getSslTrustStorePassword() {
		return sslTrustStorePassword;
	}
	
	public void setSslTrustStorePassword(GuardedString sslTrustStorePassword) {
		this.sslTrustStorePassword = sslTrustStorePassword;
	}

	@ConfigurationProperty(order = 62, displayMessageKey = "sslTrustStoreProvider.display",
			helpMessageKey = "sslTrustStoreProvider.help", required = false, confidential = false)
	public String getSslTrustStoreProvider() {
		return sslTrustStoreProvider;
	}
	
	public void setSslTrustStoreProvider(String sslTrustStoreProvider) {
		this.sslTrustStoreProvider = sslTrustStoreProvider;
	}

	@ConfigurationProperty(order = 63, displayMessageKey = "sslTrustManagerFactoryProvider.display",
			helpMessageKey = "sslTrustManagerFactoryProvider.help", required = false, confidential = false)
	public String getSslTrustManagerFactoryProvider() {
		return sslTrustManagerFactoryProvider;
	}
	
	public void setSslTrustManagerFactoryProvider(String sslTrustManagerFactoryProvider) {
		this.sslTrustManagerFactoryProvider = sslTrustManagerFactoryProvider;
	}

	@ConfigurationProperty(order = 64, displayMessageKey = "sslTrustManagerFactoryAlgorithm.display",
			helpMessageKey = "sslTrustManagerFactoryAlgorithm.help", required = false, confidential = false)
	public String getSslTrustManagerFactoryAlgorithm() {
		return sslTrustManagerFactoryAlgorithm;
	}
	
	public void setSslTrustManagerFactoryAlgorithm(String sslTrustManagerFactoryAlgorithm) {
		this.sslTrustManagerFactoryAlgorithm = sslTrustManagerFactoryAlgorithm;
	}


	// consumer properties

	@ConfigurationProperty(order = 79, displayMessageKey = "consumerNameOfTopic.display",
			helpMessageKey = "consumerNameOfTopic.help", required = false, confidential = false)
	public String getConsumerNameOfTopic() {
		return consumerNameOfTopic;
	}

	public void setConsumerNameOfTopic(String consumerNameOfTopic) {
		this.consumerNameOfTopic = consumerNameOfTopic;
	}

	@ConfigurationProperty(order = 80, displayMessageKey = "consumerVersionOfSchema.display",
			helpMessageKey = "consumerVersionOfSchema.help", required = false, confidential = false)
	public Integer getConsumerVersionOfSchema() {
		return consumerVersionOfSchema;
	}

	public void setConsumerVersionOfSchema(Integer consumerVersionOfSchema) {
		this.consumerVersionOfSchema = consumerVersionOfSchema;
	}

	@ConfigurationProperty(order = 81, displayMessageKey = "consumerGroupId.display",
			helpMessageKey = "consumerGroupId.help", required = false, confidential = false)
	public String getConsumerGroupId() {
		return consumerGroupId;
	}

	public void setConsumerGroupId(String consumerGroupId) {
		this.consumerGroupId = consumerGroupId;
	}
	
	@ConfigurationProperty(order = 83, displayMessageKey = "consumerPartitionOfTopic.display",
			helpMessageKey = "consumerPartitionOfTopic.help", required = false, confidential = false)
	public String getConsumerPartitionOfTopic() {
		return consumerPartitionOfTopic;
	}
	
	public void setConsumerPartitionOfTopic(String partitionOfTopic) {
		this.consumerPartitionOfTopic = partitionOfTopic;
	}

	@ConfigurationProperty(order = 87, displayMessageKey = "consumerDurationIfFail.display",
			helpMessageKey = "consumerDurationIfFail.help", required = false, confidential = false)
	public Integer getConsumerDurationIfFail() {
		return consumerDurationIfFail;
	}
	
	public void setConsumerDurationIfFail(Integer consumerDurationIfFail) {
		this.consumerDurationIfFail = consumerDurationIfFail;
	}
	
	@ConfigurationProperty(order = 88, displayMessageKey = "consumerMaxRecords.display",
			helpMessageKey = "consumerMaxRecords.help", required = false, confidential = false)
	public Integer getConsumerMaxRecords() {
		return consumerMaxRecords;
	}
	
	public void setConsumerMaxRecords(Integer consumerMaxRecords) {
		this.consumerMaxRecords = consumerMaxRecords;
	}

	@ConfigurationProperty(order = 89, displayMessageKey = "pathToMorePropertiesForConsumer.display",
			helpMessageKey = "pathToMorePropertiesForConsumer.help", required = false, confidential = false)
	public String getPathToMorePropertiesForConsumer() {
		return pathToMorePropertiesForConsumer;
	}

	public void setPathToMorePropertiesForConsumer(String pathToMorePropertiesForConsumer) {
		this.pathToMorePropertiesForConsumer = pathToMorePropertiesForConsumer;
	}

	// producer properties

	@ConfigurationProperty(order = 99, displayMessageKey = "producerNameOfTopic.display",
			helpMessageKey = "producerNameOfTopic.help", required = false, confidential = false)
	public String getProducerNameOfTopic() {
		return producerNameOfTopic;
	}

	public void setProducerNameOfTopic(String producerNameOfTopic) {
		this.producerNameOfTopic = producerNameOfTopic;
	}

	@ConfigurationProperty(order = 100, displayMessageKey = "producerPathToFileContainingSchema.display",
			helpMessageKey = "producerPathToFileContainingSchema.help", required = false, confidential = false)
	public String getProducerPathToFileContainingSchema() {
		return producerPathToFileContainingSchema;
	}

	public void setProducerPathToFileContainingSchema(String producerPathToFileContainingSchema) {
		this.producerPathToFileContainingSchema = producerPathToFileContainingSchema;
	}

	@ConfigurationProperty(order = 106, displayMessageKey = "pathToMorePropertiesForProducer.display",
			helpMessageKey = "pathToMorePropertiesForProducer.help", required = false, confidential = false)
	public String getPathToMorePropertiesForProducer() {
		return pathToMorePropertiesForProducer;
	}

	public void setPathToMorePropertiesForProducer(String pathToMorePropertiesForProducer) {
		this.pathToMorePropertiesForProducer = pathToMorePropertiesForProducer;
	}


	@Override
	public void validate() {
		LOGGER.info("Processing trough configuration validation procedure.");
		if (StringUtil.isBlank(schemaRegistryUrl)) {
			throw new ConfigurationException("Schema Registry url cannot be empty.");
		}
		if (StringUtil.isBlank(uniqueAttribute)) {
			throw new ConfigurationException("Unique attribute cannot be empty.");
		}

		if (StringUtil.isBlank(useOfConnector)) {
			throw new ConfigurationException("Use of connector attribute cannot be empty.");
		}

		if (!isConsumer() && !isProducer()) {
			throw new ConfigurationException("Use of connector attribute has unexpected value " + useOfConnector
			+ ", expected value is " + UseOfConnector.CONSUMER.name() + ", " + UseOfConnector.CONSUMER.name() +
					" or " + UseOfConnector.CONSUMER_AND_PRODUCER.name());
		}

		if (bootstrapServers == null || bootstrapServers.isEmpty()) {
			throw new ConfigurationException("Consumer bootstrap server cannot be empty.");
		}

		if (StringUtil.isBlank(nameOfSchema)) {
			throw new ConfigurationException("Consumer name of schema cannot be empty.");
		}

		if (isConsumer()) {
			if (StringUtil.isBlank(consumerNameOfTopic)) {
				throw new ConfigurationException("Consumer name of topic for consumer cannot be empty.");
			}

			if (consumerVersionOfSchema == null) {
				throw new ConfigurationException("Consumer version of schema cannot be empty.");
			}
			if (StringUtil.isBlank(consumerGroupId)) {
				throw new ConfigurationException("Consumer grouper id for consumer cannot be empty.");
			}
		}

		if (isProducer()) {

			if (StringUtil.isBlank(producerNameOfTopic)) {
				throw new ConfigurationException("Producer name of topic for consumer cannot be empty.");
			}

			if (StringUtil.isBlank(producerPathToFileContainingSchema)) {
				throw new ConfigurationException("Producer path to file containing schema cannot be empty.");
			}
		}


		LOGGER.info("Configuration valid");
	}
	
	@Override
	public void release() {
		LOGGER.info("The release of configuration resources is being performed");
		this.schemaRegistryUrl = null;
		this.schemaRegistrySslProtocol = null;

		this.ssoUrlRenewal = null;
		this.serviceUrlRenewal = null;
		this.usernameRenewal = null;
		this.passwordRenewal = null;
		this.clientIdRenewal = null;
		this.intervalForCertificateRenewal = null;
		this.sslPrivateKeyEntryAlias = null;
		this.sslPrivateKeyEntryPassword = null;
		this.sslTrustCertificateAliasPrefix = null;

		this.useOfConnector = null;
		this.uniqueAttribute = null;
		this.nameAttribute = null;
		this.passwordAttribute = null;
		this.bootstrapServers = null;
		this.nameOfSchema = null;
		this.kafkaSecurityProtocol = null;
		this.sslKeyStoreType = null;
		this.sslKeyStorePath = null;
		this.sslKeyStorePassword = null;
		this.sslKeyStoreProvider = null;
		this.sslKeyPassword = null;
		this.sslKeyManagerFactoryProvider = null;
		this.sslKeyManagerFactoryAlgorithm = null;
		this.sslTrustStoreType = null;
		this.sslTrustStorePath = null;
		this.sslTrustStorePassword = null;
		this.sslTrustStoreProvider = null;
		this.sslTrustManagerFactoryProvider = null;
		this.sslTrustManagerFactoryAlgorithm = null;

		this.consumerNameOfTopic = null;
		this.consumerVersionOfSchema = null;
		this.consumerPartitionOfTopic = null;
		this.consumerGroupId = null;
		this.consumerDurationIfFail = null;
		this.consumerMaxRecords = null;
		this.pathToMorePropertiesForConsumer = null;

		this.producerNameOfTopic = null;
		this.producerPathToFileContainingSchema = null;
		this.pathToMorePropertiesForProducer = null;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("KafkaConnectorConfiguration{")
				.append("schemaRegistryUrl='").append(schemaRegistryUrl).append("'");
		writeOptionalProperties(sb, "pathToMorePropertiesForSchemaRegistry", pathToMorePropertiesForSchemaRegistry);
		writeOptionalProperties(sb, "schemaRegistrySslProtocol", schemaRegistrySslProtocol);

		writeOptionalProperties(sb, "ssoUrlRenewal", ssoUrlRenewal);
		writeOptionalProperties(sb, "serviceUrlRenewal", serviceUrlRenewal);
		writeOptionalProperties(sb, "usernameRenewal", usernameRenewal);
		writeOptionalProperties(sb, "passwordRenewal", passwordRenewal);
		writeOptionalProperties(sb, "clientIdRenewal", clientIdRenewal);
		writeOptionalProperties(sb, "clientIdRenewal", intervalForCertificateRenewal);
		writeOptionalProperties(sb, "sslPrivateKeyEntryAlias", sslPrivateKeyEntryAlias);
		writeOptionalProperties(sb, "sslPrivateKeyEntryPassword", sslPrivateKeyEntryPassword);
		writeOptionalProperties(sb, "sslTrustCertificateAlias", sslTrustCertificateAliasPrefix);

		sb.append(", useOfConnector='").append(useOfConnector).append("'")
			.append(", bootstrapServers='").append(bootstrapServers).append("'")
			.append(", nameOfSchema='").append(nameOfSchema).append("'")
			.append(", uniqueAttribute='").append(uniqueAttribute).append("'");
		writeOptionalProperties(sb, "nameAttribute", nameAttribute);
		writeOptionalProperties(sb, "passwordAttribute", passwordAttribute);
		writeOptionalProperties(sb, "kafkaSecurityProtocol", kafkaSecurityProtocol);
		writeOptionalProperties(sb, "sslKeyStoreType", sslKeyStoreType);
		writeOptionalProperties(sb, "sslKeyStorePath", sslKeyStorePath);
		writeOptionalProperties(sb, "sslKeyStorePassword", sslKeyStorePassword);
		writeOptionalProperties(sb, "sslKeyStoreProvider", sslKeyStoreProvider);
		writeOptionalProperties(sb, "sslKeyPassword", sslKeyPassword);
		writeOptionalProperties(sb, "sslKeyManagerFactoryProvider", sslKeyManagerFactoryProvider);
		writeOptionalProperties(sb, "sslKeyManagerFactoryAlgorithm", sslKeyManagerFactoryAlgorithm);
		writeOptionalProperties(sb, "sslTrustStoreType", sslTrustStoreType);
		writeOptionalProperties(sb, "sslTrustStorePath", sslTrustStorePath);
		writeOptionalProperties(sb, "sslTrustStorePassword", sslTrustStorePassword);
		writeOptionalProperties(sb, "sslTrustStoreProvider", sslTrustStoreProvider);
		writeOptionalProperties(sb, "sslTrustManagerFactoryProvider", sslTrustManagerFactoryProvider);
		writeOptionalProperties(sb, "sslTrustManagerFactoryAlgorithm", sslTrustManagerFactoryAlgorithm);


		if (isConsumer()) {
			sb.append(", consumerNameOfTopic='").append(consumerNameOfTopic).append("'")
					.append(", consumerVersionOfSchema='").append(consumerVersionOfSchema).append("'");
			writeOptionalProperties(sb, "consumerPartitionOfTopic", consumerPartitionOfTopic);
			writeOptionalProperties(sb, "consumerGroupId", consumerGroupId);
			writeOptionalProperties(sb, "consumerDurationIfFail", consumerDurationIfFail);
			writeOptionalProperties(sb, "consumerMaxRecords", consumerMaxRecords);
			writeOptionalProperties(sb, "pathToMorePropertiesForConsumer", pathToMorePropertiesForConsumer);
		}

		if (isProducer()) {
			sb.append(", producerNameOfTopic='").append(producerNameOfTopic).append("'")
					.append(", producerPathToFileContainingSchema='").append(producerPathToFileContainingSchema).append("'");
			writeOptionalProperties(sb, "pathToMorePropertiesForProducer", pathToMorePropertiesForProducer);
		}
		sb.append("}");
		return sb.toString();
	}
	
	private static void writeOptionalProperties(StringBuilder sb, String name, Object value) {
		if(value != null) {
			sb.append(", ").append(name).append("=").append(value).append("'");
		}
	}
	
	private static void writeOptionalProperties(StringBuilder sb, String name, String value) {
		if(StringUtil.isNotBlank(value)) {
			sb.append(", ").append(name).append("=").append(value).append("'");
		}
	}
	
	private static void writeOptionalProperties(StringBuilder sb, String name, GuardedString value) {
		if (value != null) {
			final StringBuilder password = new StringBuilder();
			sb.append(", ").append(name).append("=").append("[hidden]");
		}
	}
	
	public boolean isValidConfigForRenewal(Log logger) {
		if(StringUtil.isBlank(sslKeyStorePath)) {
			logger.info("sslKeyStorePath is blank");
			return false;
		}
		if(sslKeyStorePassword == null) {
			logger.info("sslKeyStorePassword is null");
			return false;
		}
		if(sslPrivateKeyEntryPassword == null) {
			logger.info("sslPrivateKeyEntryPassword is null");
			return false;
		}
		if(StringUtil.isBlank(sslTrustStorePath)) {
			logger.info("sslTrustStorePath is blank");
			return false;
		}
		if(sslTrustStorePassword == null) {
			logger.info("sslTrustStorePassword is null");
			return false;
		}
		if(StringUtil.isBlank(ssoUrlRenewal)) {
			logger.info("ssoUrlRenewal is blank");
			return false;
		}
		if(StringUtil.isBlank(serviceUrlRenewal)) {
			logger.info("serviceUrlRenewal is blank");
			return false;
		}
		if(StringUtil.isBlank(usernameRenewal)) {
			logger.info("usernameRenewal is blank");
			return false;
		}
		if(passwordRenewal == null) {
			logger.info("passwordRenewal is null");
			return false;
		}
		if(StringUtil.isBlank(clientIdRenewal)) {
			logger.info("clientIdRenewal is blank");
			return false;
		}
		return true;
	}

	public boolean isConsumer() {
		return UseOfConnector.CONSUMER.name().equals(useOfConnector) || UseOfConnector.CONSUMER_AND_PRODUCER.name().equals(useOfConnector);
	}

	public boolean isProducer() {
		return UseOfConnector.PRODUCER.name().equals(useOfConnector) || UseOfConnector.CONSUMER_AND_PRODUCER.name().equals(useOfConnector);
	}
}