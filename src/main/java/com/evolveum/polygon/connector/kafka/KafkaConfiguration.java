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

	private String schemaRegistryUrl;
	private String nameOfSchema;
	private Integer versionOfSchema;
	private String uniqueAttribute;
	private String nameAttribute;
	private Integer schemaRegistryClassLoaderCacheSize;
	private Integer schemaRegistryClassLoaderCacheExpiryInterval;
	private Integer schemaRegistrySchemaVersionCacheSize;
	private Integer schemaRegistrySchemaVersionCacheExpiryInterval;
	private Integer schemaRegistrySchemaMetadataCacheSize;
	private Integer schemaRegistrySchemaMetadataCacheExpiryInterval;
	private Integer schemaRegistrySchemaTextCacheSize;
	private Integer schemaRegistrySchemaTextCacheExpiryInterval;
	private String schemaRegistrySslProtocol;
	private String consumerBootstrapServers;
	private String consumerNameOfTopic;
	private String consumerGroupId;
	private String consumerPartitionOfTopic;
	private String consumerSecurityProtocol;
	private String consumerDurationIfFail;
	private Integer consumerMaxRecords;
	private String pathToMorePropertiesForConsumer;
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
	private String sslPrivateKeyEntryAlias;
	private GuardedString sslPrivateKeyEntryPassword;
	private String sslTrustCertificateAliasPrefix;
	private String ssoUrlRenewal;
	private String serviceUrlRenewal;
	private String usernameRenewal;
	private GuardedString passwordRenewal;
	private String clientIdRenewal;
	private Integer intervalForCertificateRenewal;
	
	
	private static final Log LOGGER = Log.getLog(KafkaConnector.class);

	@ConfigurationProperty(order = 3, displayMessageKey = "schemaRegistryUrl.display",
			helpMessageKey = "schemaRegistryUrl.help", required = true, confidential = false)
	public String getSchemaRegistryUrl() {
		return schemaRegistryUrl;
	}
	
	public void setSchemaRegistryUrl(String schemaRegistryUrl) {
		this.schemaRegistryUrl = schemaRegistryUrl;
	}
	
	@ConfigurationProperty(order = 4, displayMessageKey = "nameOfSchema.display",
			helpMessageKey = "nameOfSchema.help", required = true, confidential = false)
	public String getNameOfSchema() {
		return nameOfSchema;
	}
	
	public void setNameOfSchema(String nameOfSchema) {
		this.nameOfSchema = nameOfSchema;
	}
	
	@ConfigurationProperty(order = 5, displayMessageKey = "versionOfSchema.display",
			helpMessageKey = "versionOfSchema.help", required = true, confidential = false)
	public Integer getVersionOfSchema() {
		return versionOfSchema;
	}
	
	public void setVersionOfSchema(Integer versionOfSchema) {
		this.versionOfSchema = versionOfSchema;
	}
	
	@ConfigurationProperty(order = 6, displayMessageKey = "uniqueAttribute.display",
			helpMessageKey = "uniqueAttribute.help", required = true, confidential = false)
	public String getUniqueAttribute() {
		return uniqueAttribute;
	}
	
	public void setUniqueAttribute(String uniqueAttribute) {
		this.uniqueAttribute = uniqueAttribute;
	}
	
	@ConfigurationProperty(order = 8, displayMessageKey = "nameAttribute.display",
			helpMessageKey = "nameAttribute.help", required = false, confidential = false)
	public String getNameAttribute() {
		return nameAttribute;
	}
	
	public void setNameAttribute(String nameAttribute) {
		this.nameAttribute = nameAttribute;
	}
	
	@ConfigurationProperty(order = 9, displayMessageKey = "schemaRegistryClassLoaderCacheSize.display",
			helpMessageKey = "schemaRegistryClassLoaderCacheSize.help", required = false, confidential = false)
	public Integer getSchemaRegistryClassLoaderCacheSize() {
		return schemaRegistryClassLoaderCacheSize;
	}
	
	public void setSchemaRegistryClassLoaderCacheSize(Integer classLoaderCacheSize) {
		this.schemaRegistryClassLoaderCacheSize = classLoaderCacheSize;
	}
	
	@ConfigurationProperty(order = 10, displayMessageKey = "schemaRegistryClassLoaderCacheExpiryInterval.display",
			helpMessageKey = "schemaRegistryClassLoaderCacheExpiryInterval.help", required = false, confidential = false)
	public Integer getSchemaRegistryClassLoaderCacheExpiryInterval() {
		return schemaRegistryClassLoaderCacheExpiryInterval;
	}
	
	public void setSchemaRegistryClassLoaderCacheExpiryInterval(Integer classLoaderCacheExpiryInterval) {
		this.schemaRegistryClassLoaderCacheExpiryInterval = classLoaderCacheExpiryInterval;
	}
	
	@ConfigurationProperty(order = 11, displayMessageKey = "schemaRegistrySchemaVersionCacheSize.display",
			helpMessageKey = "schemaRegistrySchemaVersionCacheSize.help", required = false, confidential = false)
	public Integer getSchemaRegistrySchemaVersionCacheSize() {
		return schemaRegistrySchemaVersionCacheSize;
	}
	
	public void setSchemaRegistrySchemaVersionCacheSize(Integer schemaVersionCacheSize) {
		this.schemaRegistrySchemaVersionCacheSize = schemaVersionCacheSize;
	}
	
	@ConfigurationProperty(order = 12, displayMessageKey = "schemaRegistrySchemaVersionCacheExpiryInterval.display",
			helpMessageKey = "schemaRegistrySchemaVersionCacheExpiryInterval.help", required = false, confidential = false)
	public Integer getSchemaRegistrySchemaVersionCacheExpiryInterval() {
		return schemaRegistrySchemaVersionCacheExpiryInterval;
	}
	
	public void setSchemaRegistrySchemaVersionCacheExpiryInterval(Integer schemaVersionCacheExpiryInterval) {
		this.schemaRegistrySchemaVersionCacheExpiryInterval = schemaVersionCacheExpiryInterval;
	}
	
	@ConfigurationProperty(order = 13, displayMessageKey = "schemaRegistrySchemaMetadataCacheSize.display",
			helpMessageKey = "schemaRegistrySchemaMetadataCacheSize.help", required = false, confidential = false)
	public Integer getSchemaRegistrySchemaMetadataCacheSize() {
		return schemaRegistrySchemaMetadataCacheSize;
	}
	
	public void setSchemaRegistrySchemaMetadataCacheSize(Integer schemaMetadataCacheSize) {
		this.schemaRegistrySchemaMetadataCacheSize = schemaMetadataCacheSize;
	}
	
	@ConfigurationProperty(order = 14, displayMessageKey = "schemaRegistrySchemaMetadataCacheExpiryInterval.display",
			helpMessageKey = "schemaRegistrySchemaMetadataCacheExpiryInterval.help", required = false, confidential = false)
	public Integer getSchemaRegistrySchemaMetadataCacheExpiryInterval() {
		return schemaRegistrySchemaMetadataCacheExpiryInterval;
	}
	
	public void setSchemaRegistrySchemaMetadataCacheExpiryInterval(Integer schemaMetadataCacheExpiryInterval) {
		this.schemaRegistrySchemaMetadataCacheExpiryInterval = schemaMetadataCacheExpiryInterval;
	}

	@ConfigurationProperty(order = 15, displayMessageKey = "schemaRegistrySchemaTextCacheSize.display",
			helpMessageKey = "schemaRegistrySchemaTextCacheSize.help", required = false, confidential = false)
	public Integer getSchemaRegistrySchemaTextCacheSize() {
		return schemaRegistrySchemaTextCacheSize;
	}
	
	public void setSchemaRegistrySchemaTextCacheSize(Integer schemaTextCacheSize) {
		this.schemaRegistrySchemaTextCacheSize = schemaTextCacheSize;
	}

	@ConfigurationProperty(order = 16, displayMessageKey = "schemaRegistrySchemaTextCacheExpiryInterval.display",
			helpMessageKey = "schemaRegistrySchemaTextCacheExpiryInterval.help", required = false, confidential = false)
	public Integer getSchemaRegistrySchemaTextCacheExpiryInterval() {
		return schemaRegistrySchemaTextCacheExpiryInterval;
	}
	
	public void setSchemaRegistrySchemaTextCacheExpiryInterval(Integer schemaTextCacheExpiryInterval) {
		this.schemaRegistrySchemaTextCacheExpiryInterval = schemaTextCacheExpiryInterval;
	}

	@ConfigurationProperty(order = 17, displayMessageKey = "schemaRegistrySslProtocol.display",
			helpMessageKey = "schemaRegistrySslProtocol.help", required = false, confidential = false)
	public String getSchemaRegistrySslProtocol() {
		return schemaRegistrySslProtocol;
	}
	
	public void setSchemaRegistrySslProtocol(String sslProtocol) {
		this.schemaRegistrySslProtocol = sslProtocol;
	}

	@ConfigurationProperty(order = 18, displayMessageKey = "sslKeyStoreType.display",
			helpMessageKey = "sslKeyStoreType.help", required = false, confidential = false)
	public String getSslKeyStoreType() {
		return sslKeyStoreType;
	}
	
	public void setSslKeyStoreType(String sslKeyStoreType) {
		this.sslKeyStoreType = sslKeyStoreType;
	}

	@ConfigurationProperty(order = 19, displayMessageKey = "sslKeyStorePath.display",
			helpMessageKey = "sslKeyStorePath.help", required = false, confidential = false)
	public String getSslKeyStorePath() {
		return sslKeyStorePath;
	}
	
	public void setSslKeyStorePath(String sslKeyStorePath) {
		this.sslKeyStorePath = sslKeyStorePath;
	}

	@ConfigurationProperty(order = 20, displayMessageKey = "sslKeyStorePassword.display",
			helpMessageKey = "sslKeyStorePassword.help", required = false, confidential = true)
	public GuardedString getSslKeyStorePassword() {
		return sslKeyStorePassword;
	}
	
	public void setSslKeyStorePassword(GuardedString sslKeyStorePassword) {
		this.sslKeyStorePassword = sslKeyStorePassword;
	}
	
	@ConfigurationProperty(order = 21, displayMessageKey = "sslKeyStoreProvider.display",
			helpMessageKey = "sslKeyStoreProvider.help", required = false, confidential = false)
	public String getSslKeyStoreProvider() {
		return sslKeyStoreProvider;
	}
	
	public void setSslKeyStoreProvider(String sslKeyStoreProvider) {
		this.sslKeyStoreProvider = sslKeyStoreProvider;
	}

	@ConfigurationProperty(order = 22, displayMessageKey = "sslKeyPassword.display",
			helpMessageKey = "sslKeyPassword.help", required = false, confidential = true)
	public GuardedString getSslKeyPassword() {
		return sslKeyPassword;
	}
	
	public void setSslKeyPassword(GuardedString sslKeyPassword) {
		this.sslKeyPassword = sslKeyPassword;
	}

	@ConfigurationProperty(order = 23, displayMessageKey = "sslKeyManagerFactoryProvider.display",
			helpMessageKey = "sslKeyManagerFactoryProvider.help", required = false, confidential = false)
	public String getSslKeyManagerFactoryProvider() {
		return sslKeyManagerFactoryProvider;
	}
	
	public void setSslKeyManagerFactoryProvider(String sslKeyManagerFactoryProvider) {
		this.sslKeyManagerFactoryProvider = sslKeyManagerFactoryProvider;
	}

	@ConfigurationProperty(order = 24, displayMessageKey = "sslKeyManagerFactoryAlgorithm.display",
			helpMessageKey = "sslKeyManagerFactoryAlgorithm.help", required = false, confidential = false)
	public String getSslKeyManagerFactoryAlgorithm() {
		return sslKeyManagerFactoryAlgorithm;
	}
	
	public void setSslKeyManagerFactoryAlgorithm(String sslKeyManagerFactoryAlgorithm) {
		this.sslKeyManagerFactoryAlgorithm = sslKeyManagerFactoryAlgorithm;
	}

	@ConfigurationProperty(order = 25, displayMessageKey = "sslTrustStoreType.display",
			helpMessageKey = "sslTrustStoreType.help", required = false, confidential = false)
	public String getSslTrustStoreType() {
		return sslTrustStoreType;
	}
	
	public void setSslTrustStoreType(String sslTrustStoreType) {
		this.sslTrustStoreType = sslTrustStoreType;
	}

	@ConfigurationProperty(order = 26, displayMessageKey = "sslTrustStorePath.display",
			helpMessageKey = "sslTrustStorePath.help", required = false, confidential = false)
	public String getSslTrustStorePath() {
		return sslTrustStorePath;
	}
	
	public void setSslTrustStorePath(String sslTrustStorePath) {
		this.sslTrustStorePath = sslTrustStorePath;
	}

	@ConfigurationProperty(order = 27, displayMessageKey = "sslTrustStorePassword.display",
			helpMessageKey = "sslTrustStorePassword.help", required = false, confidential = true)
	public GuardedString getSslTrustStorePassword() {
		return sslTrustStorePassword;
	}
	
	public void setSslTrustStorePassword(GuardedString sslTrustStorePassword) {
		this.sslTrustStorePassword = sslTrustStorePassword;
	}

	@ConfigurationProperty(order = 28, displayMessageKey = "sslTrustStoreProvider.display",
			helpMessageKey = "sslTrustStoreProvider.help", required = false, confidential = false)
	public String getSslTrustStoreProvider() {
		return sslTrustStoreProvider;
	}
	
	public void setSslTrustStoreProvider(String sslTrustStoreProvider) {
		this.sslTrustStoreProvider = sslTrustStoreProvider;
	}

	@ConfigurationProperty(order = 29, displayMessageKey = "sslTrustManagerFactoryProvider.display",
			helpMessageKey = "sslTrustManagerFactoryProvider.help", required = false, confidential = false)
	public String getSslTrustManagerFactoryProvider() {
		return sslTrustManagerFactoryProvider;
	}
	
	public void setSslTrustManagerFactoryProvider(String sslTrustManagerFactoryProvider) {
		this.sslTrustManagerFactoryProvider = sslTrustManagerFactoryProvider;
	}

	@ConfigurationProperty(order = 30, displayMessageKey = "sslTrustManagerFactoryAlgorithm.display",
			helpMessageKey = "sslTrustManagerFactoryAlgorithm.help", required = false, confidential = false)
	public String getSslTrustManagerFactoryAlgorithm() {
		return sslTrustManagerFactoryAlgorithm;
	}
	
	public void setSslTrustManagerFactoryAlgorithm(String sslTrustManagerFactoryAlgorithm) {
		this.sslTrustManagerFactoryAlgorithm = sslTrustManagerFactoryAlgorithm;
	}
	
	@ConfigurationProperty(order = 36, displayMessageKey = "sslPrivateKeyEntryAlias.display",
			helpMessageKey = "sslPrivateKeyEntryAlias.help", required = false, confidential = false)
	public String getSslPrivateKeyEntryAlias() {
		return sslPrivateKeyEntryAlias;
	}
	
	public void setSslPrivateKeyEntryAlias(String sslPrivateKeyEntryAlias) {
		this.sslPrivateKeyEntryAlias = sslPrivateKeyEntryAlias;
	}
	
	@ConfigurationProperty(order = 37, displayMessageKey = "sslPrivateKeyEntryPassword.display",
			helpMessageKey = "sslPrivateKeyEntryPassword.help", required = false, confidential = false)
	public GuardedString getSslPrivateKeyEntryPassword() {
		return sslPrivateKeyEntryPassword;
	}
	
	public void setSslPrivateKeyEntryPassword(GuardedString sslPrivateKeyEntryPassword) {
		this.sslPrivateKeyEntryPassword = sslPrivateKeyEntryPassword;
	}
	
	@ConfigurationProperty(order = 38, displayMessageKey = "sslTrustCertificateAliasPrefix.display",
			helpMessageKey = "sslTrustCertificateAliasPrefix.help", required = false, confidential = false)
	public String getSslTrustCertificateAliasPrefix() {
		return sslTrustCertificateAliasPrefix;
	}
	
	public void setSslTrustCertificateAliasPrefix(String sslTrustCertificateAliasPrefix) {
		this.sslTrustCertificateAliasPrefix = sslTrustCertificateAliasPrefix;
	}
	
	@ConfigurationProperty(order = 41, displayMessageKey = "consumerBootstrapServers.display",
			helpMessageKey = "consumerBootstrapServers.help", required = true, confidential = false)
	public String getConsumerBootstrapServers() {
		return consumerBootstrapServers;
	}
	
	public void setConsumerBootstrapServers(String consumerBootstrapServers) {
		this.consumerBootstrapServers = consumerBootstrapServers;
	}
	
	@ConfigurationProperty(order = 42, displayMessageKey = "consumerNameOfTopic.display",
			helpMessageKey = "consumerNameOfTopic.help", required = true, confidential = false)
	public String getConsumerNameOfTopic() {
		return consumerNameOfTopic;
	}
	
	public void setConsumerNameOfTopic(String nameOfTopic) {
		this.consumerNameOfTopic = nameOfTopic;
	}
	
	@ConfigurationProperty(order = 43, displayMessageKey = "consumerPartitionOfTopic.display",
			helpMessageKey = "consumerPartitionOfTopic.help", required = true, confidential = false)
	public String getConsumerPartitionOfTopic() {
		return consumerPartitionOfTopic;
	}
	
	public void setConsumerPartitionOfTopic(String partitionOfTopic) {
		this.consumerPartitionOfTopic = partitionOfTopic;
	}
	
	@ConfigurationProperty(order = 44, displayMessageKey = "consumerGroupId.display",
			helpMessageKey = "consumerGroupId.help", required = true, confidential = false)
	public String getConsumerGroupId() {
		return consumerGroupId;
	}
	
	public void setConsumerGroupId(String consumerGroupId) {
		this.consumerGroupId = consumerGroupId;
	}
	
	@ConfigurationProperty(order = 45, displayMessageKey = "pathToMorePropertiesForConsumer.display",
			helpMessageKey = "pathToMorePropertiesForConsumer.help", required = false, confidential = false)
	public String getPathToMorePropertiesForConsumer() {
		return pathToMorePropertiesForConsumer;
	}
	
	public void setPathToMorePropertiesForConsumer(String pathToMorePropertiesForConsumer) {
		this.pathToMorePropertiesForConsumer = pathToMorePropertiesForConsumer;
	}
	
	@ConfigurationProperty(order = 46, displayMessageKey = "consumerSecurityProtocol.display",
			helpMessageKey = "consumerSecurityProtocol.help", required = false, confidential = false)
	public String getConsumerSecurityProtocol() {
		return consumerSecurityProtocol;
	}
	
	public void setConsumerSecurityProtocol(String consumerSecurityProtocol) {
		this.consumerSecurityProtocol = consumerSecurityProtocol;
	}
	
	@ConfigurationProperty(order = 47, displayMessageKey = "consumerDurationIfFail.display",
			helpMessageKey = "consumerDurationIfFail.help", required = false, confidential = false)
	public String getConsumerDurationIfFail() {
		return consumerDurationIfFail;
	}
	
	public void setConsumerDurationIfFail(String consumerDurationIfFail) {
		this.consumerDurationIfFail = consumerDurationIfFail;
	}
	
	@ConfigurationProperty(order = 48, displayMessageKey = "consumerMaxRecords.display",
			helpMessageKey = "consumerMaxRecords.help", required = false, confidential = false)
	public Integer getConsumerMaxRecords() {
		return consumerMaxRecords;
	}
	
	public void setConsumerMaxRecords(Integer consumerMaxRecords) {
		this.consumerMaxRecords = consumerMaxRecords;
	}
	
	@ConfigurationProperty(order = 49, displayMessageKey = "ssoUrlRenewal.display",
			helpMessageKey = "ssoUrlRenewal.help", required = false, confidential = false)
	public String getSsoUrlRenewal() {
		return ssoUrlRenewal;
	}
	
	public void setSsoUrlRenewal(String ssoUrlRenewal) {
		this.ssoUrlRenewal = ssoUrlRenewal;
	}
	
	@ConfigurationProperty(order = 50, displayMessageKey = "serviceUrlRenewal.display",
			helpMessageKey = "serviceUrlRenewal.help", required = false, confidential = false)
	public String getServiceUrlRenewal() {
		return serviceUrlRenewal;
	}
	
	public void setServiceUrlRenewal(String serviceUrlRenewal) {
		this.serviceUrlRenewal = serviceUrlRenewal;
	}
	
	@ConfigurationProperty(order = 51, displayMessageKey = "usernameRenewal.display",
			helpMessageKey = "usernameRenewal.help", required = false, confidential = false)
	public String getUsernameRenewal() {
		return usernameRenewal;
	}
	
	public void setUsernameRenewal(String usernameRenewal) {
		this.usernameRenewal = usernameRenewal;
	}
	
	@ConfigurationProperty(order = 52, displayMessageKey = "passwordRenewal.display",
			helpMessageKey = "passwordRenewal.help", required = false, confidential = false)
	public GuardedString getPasswordRenewal() {
		return passwordRenewal;
	}
	
	public void setPasswordRenewal(GuardedString passwordRenewal) {
		this.passwordRenewal = passwordRenewal;
	}
	
	@ConfigurationProperty(order = 53, displayMessageKey = "clientIdRenewal.display",
			helpMessageKey = "clientIdRenewal.help", required = false, confidential = false)
	public String getClientIdRenewal() {
		return clientIdRenewal;
	}
	
	public void setClientIdRenewal(String clientIdRenewal) {
		this.clientIdRenewal = clientIdRenewal;
	}
	
	@ConfigurationProperty(order = 54, displayMessageKey = "intervalForCertificateRenewal.display",
			helpMessageKey = "intervalForCertificateRenewal.help", required = false, confidential = false)
	public Integer getIntervalForCertificateRenewal() {
		return intervalForCertificateRenewal;
	}
	
	public void setIntervalForCertificateRenewal(Integer intervalForCertificateRenewal) {
		this.intervalForCertificateRenewal = intervalForCertificateRenewal;
	}
	
	@Override
	public void validate() {
		LOGGER.info("Processing trough configuration validation procedure.");
		if (StringUtil.isBlank(schemaRegistryUrl)) {
			throw new ConfigurationException("Schema Registry url cannot be empty.");
		}
		if (consumerBootstrapServers == null || consumerBootstrapServers.isEmpty()) {
			throw new ConfigurationException("Bootstrap server cannot be empty.");
		}
		if (StringUtil.isBlank(nameOfSchema)) {
			throw new ConfigurationException("Name of schema cannot be empty.");
		}
		if (versionOfSchema == null) {
			throw new ConfigurationException("Version of schema cannot be empty.");
		}
		if (StringUtil.isBlank(consumerNameOfTopic)) {
			throw new ConfigurationException("Name of topic for consumer cannot be empty.");
		}
		if (StringUtil.isBlank(uniqueAttribute)) {
			throw new ConfigurationException("Unique attribute cannot be empty.");
		}
		
		if (StringUtil.isBlank(consumerGroupId)) {
			throw new ConfigurationException("Grouper id for consumer cannot be empty.");
		}
		
		LOGGER.info("Configuration valid");
	}
	
	@Override
	public void release() {
		LOGGER.info("The release of configuration resources is being performed");
		this.schemaRegistryUrl = null;
		this.consumerBootstrapServers = null;
		this.nameOfSchema = null;
		this.versionOfSchema = null;
		this.consumerNameOfTopic = null;
		this.uniqueAttribute = null;
		this.nameAttribute = null;
		this.schemaRegistryClassLoaderCacheSize = null;
		this.schemaRegistryClassLoaderCacheExpiryInterval = null;
		this.schemaRegistrySchemaVersionCacheSize = null;
		this.schemaRegistrySchemaVersionCacheExpiryInterval = null;
		this.schemaRegistrySchemaMetadataCacheExpiryInterval = null;
		this.schemaRegistrySchemaTextCacheSize = null;
		this.schemaRegistrySchemaTextCacheExpiryInterval = null;
		this.schemaRegistrySslProtocol = null;
		this.pathToMorePropertiesForConsumer = null;
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
		this.consumerGroupId = null;
		this.consumerPartitionOfTopic = null;
		this.sslPrivateKeyEntryAlias = null;
		this.sslPrivateKeyEntryPassword = null;
		this.sslTrustCertificateAliasPrefix = null;
		this.ssoUrlRenewal = null;
		this.serviceUrlRenewal = null;
		this.usernameRenewal = null;
		this.passwordRenewal = null;
		this.clientIdRenewal = null;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("ScimConnectorConfiguration{")
				.append("schemaRegistryUrl='").append(schemaRegistryUrl).append("'")
				.append(", nameOfSchema='").append(nameOfSchema).append("'")
				.append(", versionOfSchema='").append(versionOfSchema).append("'")
				.append(", uniqueAttribute='").append(uniqueAttribute).append("'")
				.append(", partitionOfTopic='").append(consumerPartitionOfTopic).append("'");
		writeOptionalProperties(sb, "nameAttribute", nameAttribute);
		writeOptionalProperties(sb, "schemaRegistryClassLoaderCacheSize", schemaRegistryClassLoaderCacheSize);
		writeOptionalProperties(sb, "schemaRegistryClassLoaderCacheExpiryInterval", schemaRegistryClassLoaderCacheExpiryInterval);
		writeOptionalProperties(sb, "schemaRegistrySchemaVersionCacheSize", schemaRegistrySchemaVersionCacheSize);
		writeOptionalProperties(sb, "schemaRegistrySchemaVersionCacheExpiryInterval", schemaRegistrySchemaVersionCacheExpiryInterval);
		writeOptionalProperties(sb, "schemaRegistrySchemaMetadataCacheExpiryInterval", schemaRegistrySchemaMetadataCacheExpiryInterval);
		writeOptionalProperties(sb, "schemaRegistrySchemaTextCacheSize", schemaRegistrySchemaTextCacheSize);
		writeOptionalProperties(sb, "schemaRegistrySchemaTextCacheExpiryInterval", schemaRegistrySchemaTextCacheExpiryInterval);
		writeOptionalProperties(sb, "schemaRegistrySslProtocol", schemaRegistrySslProtocol);
		writeOptionalProperties(sb, "pathToMorePropertiesForConsumer", pathToMorePropertiesForConsumer);
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
		sb.append(", consumerBootstrapServers='").append(consumerBootstrapServers).append("'")
		.append(", consumerNameOfTopic='").append(consumerNameOfTopic).append("'");
		writeOptionalProperties(sb, "consumerGroupId", consumerGroupId);
		writeOptionalProperties(sb, "sslPrivateKeyEntryAlias", sslPrivateKeyEntryAlias);
		writeOptionalProperties(sb, "sslPrivateKeyEntryPassword", sslPrivateKeyEntryPassword);
		writeOptionalProperties(sb, "sslTrustCertificateAlias", sslTrustCertificateAliasPrefix);
		writeOptionalProperties(sb, "ssoUrlRenewal", ssoUrlRenewal);
		writeOptionalProperties(sb, "serviceUrlRenewal", serviceUrlRenewal);
		writeOptionalProperties(sb, "usernameRenewal", usernameRenewal);
		writeOptionalProperties(sb, "passwordRenewal", passwordRenewal);
		writeOptionalProperties(sb, "clientIdRenewal", clientIdRenewal);
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
		final StringBuilder password = new StringBuilder();
		if (value != null) {
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
}