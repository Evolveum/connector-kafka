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
	private Integer partitionOfTopic;
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
	private Integer consumerSessionTimeoutMs;
	private Integer consumerHeartbeatIntervalMs;
	private String consumerPartitionAssignmentStrategy;
	private Long consumerMetadataMaxAgeMs;
	private String consumerClientId;
	private Integer consumerMaxPartitionFetchBytes;
	private Integer consumerSendBufferBytes;
	private Integer consumerReceiveBufferBytes;
	private Integer consumerFetchMinBytes;
	private Integer consumerFetchMaxBytes;
	private Integer consumerFetchMaxWaitMs;
	private Long consumerReconnectBackoffMs;
	private Long consumerReconnectBackoffMaxMs;
	private Long consumerRetryBackoffMs;
	private String consumerAutoOffsetReset;
	private Boolean consumerCheckCrcs;
	private Integer consumerRequestTimeoutMs;
	private Long consumerConnectionsMaxIdleMs;
	private Integer consumerMaxPollRecords;
	private Integer consumerMaxPollIntervalMs;
	private Boolean consumerExcludeInternalTopics;
	private String consumerIsolationLevel;
	private String consumerSecurityProtocol;
	private String consumerSslProtocol;
	private String consumerSslProvider;
	private String consumerSslCipherSuites;
	private String consumerSslEnabledProtocols;
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
	private String consumerSslEndpointIdentificationAlgorithm;
	private String consumerSslSecureRandomImplementation;
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
	
	@ConfigurationProperty(order = 31, displayMessageKey = "consumerBootstrapServers.display",
			helpMessageKey = "consumerBootstrapServers.help", required = true, confidential = false)
	public String getConsumerBootstrapServers() {
		return consumerBootstrapServers;
	}
	
	public void setConsumerBootstrapServers(String consumerBootstrapServers) {
		this.consumerBootstrapServers = consumerBootstrapServers;
	}
	
	@ConfigurationProperty(order = 32, displayMessageKey = "consumerNameOfTopic.display",
			helpMessageKey = "consumerNameOfTopic.help", required = true, confidential = false)
	public String getConsumerNameOfTopic() {
		return consumerNameOfTopic;
	}
	
	public void setConsumerNameOfTopic(String nameOfTopic) {
		this.consumerNameOfTopic = nameOfTopic;
	}
	
	@ConfigurationProperty(order = 33, displayMessageKey = "partitionOfTopic.display",
			helpMessageKey = "partitionOfTopic.help", required = true, confidential = false)
	public Integer getPartitionOfTopic() {
		return partitionOfTopic;
	}
	
	public void setPartitionOfTopic(Integer partitionOfTopic) {
		this.partitionOfTopic = partitionOfTopic;
	}
	
	@ConfigurationProperty(order = 34, displayMessageKey = "consumerGroupId.display",
			helpMessageKey = "consumerGroupId.help", required = true, confidential = false)
	public String getConsumerGroupId() {
		return consumerGroupId;
	}
	
	public void setConsumerGroupId(String consumerGroupId) {
		this.consumerGroupId = consumerGroupId;
	}
	
	@ConfigurationProperty(order = 35, displayMessageKey = "consumerSessionTimeoutMs.display",
			helpMessageKey = "consumerSessionTimeoutMs.help", required = false, confidential = false)
	public Integer getConsumerSessionTimeoutMs() {
		return consumerSessionTimeoutMs;
	}
	
	public void setConsumerSessionTimeoutMs(Integer consumerSessionTimeoutMs) {
		this.consumerSessionTimeoutMs = consumerSessionTimeoutMs;
	}
	
	@ConfigurationProperty(order = 36, displayMessageKey = "consumerHeartbeatIntervalMs.display",
			helpMessageKey = "consumerHeartbeatIntervalMs.help", required = false, confidential = false)
	public Integer getConsumerHeartbeatIntervalMs() {
		return consumerHeartbeatIntervalMs;
	}
	
	public void setConsumerHeartbeatIntervalMs(Integer consumerHeartbeatIntervalMs) {
		this.consumerHeartbeatIntervalMs = consumerHeartbeatIntervalMs;
	}
	
	@ConfigurationProperty(order = 37, displayMessageKey = "consumerPartitionAssignmentStrategy.display",
			helpMessageKey = "consumerPartitionAssignmentStrategy.help", required = false, confidential = false)
	public String getConsumerPartitionAssignmentStrategy() {
		return consumerPartitionAssignmentStrategy;
	}
	
	public void setConsumerPartitionAssignmentStrategy(String consumerPartitionAssignmentStrategy) {
		this.consumerPartitionAssignmentStrategy = consumerPartitionAssignmentStrategy;
	}
	
	@ConfigurationProperty(order = 38, displayMessageKey = "consumerMetadataMaxAgeMs.display",
			helpMessageKey = "consumerMetadataMaxAgeMs.help", required = false, confidential = false)
	public Long getConsumerMetadataMaxAgeMs() {
		return consumerMetadataMaxAgeMs;
	}
	
	public void setConsumerMetadataMaxAgeMs(Long consumerMetadataMaxAgeMs) {
		this.consumerMetadataMaxAgeMs = consumerMetadataMaxAgeMs;
	}
	
	@ConfigurationProperty(order = 40, displayMessageKey = "consumerClientId.display",
			helpMessageKey = "consumerClientId.help", required = false, confidential = false)
	public String getConsumerClientId() {
		return consumerClientId;
	}
	
	public void setConsumerClientId(String consumerClientId) {
		this.consumerClientId = consumerClientId;
	}
	
	@ConfigurationProperty(order = 41, displayMessageKey = "consumerMaxPartitionFetchBytes.display",
			helpMessageKey = "consumerMaxPartitionFetchBytes.help", required = false, confidential = false)
	public Integer getConsumerMaxPartitionFetchBytes() {
		return consumerMaxPartitionFetchBytes;
	}
	
	public void setConsumerMaxPartitionFetchBytes(Integer consumerMaxPartitionFetchBytes) {
		this.consumerMaxPartitionFetchBytes = consumerMaxPartitionFetchBytes;
	}
	
	@ConfigurationProperty(order = 42, displayMessageKey = "consumerSendBufferBytes.display",
			helpMessageKey = "consumerSendBufferBytes.help", required = false, confidential = false)
	public Integer getConsumerSendBufferBytes() {
		return consumerSendBufferBytes;
	}
	
	public void setConsumerSendBufferBytes(Integer consumerSendBufferBytes) {
		this.consumerSendBufferBytes = consumerSendBufferBytes;
	}
	
	@ConfigurationProperty(order = 43, displayMessageKey = "consumerReceiveBufferBytes.display",
			helpMessageKey = "consumerReceiveBufferBytes.help", required = false, confidential = false)
	public Integer getConsumerReceiveBufferBytes() {
		return consumerReceiveBufferBytes;
	}
	
	public void setConsumerReceiveBufferBytes(Integer consumerReceiveBufferBytes) {
		this.consumerReceiveBufferBytes = consumerReceiveBufferBytes;
	}
	
	@ConfigurationProperty(order = 44, displayMessageKey = "consumerFetchMinBytes.display",
			helpMessageKey = "consumerFetchMinBytes.help", required = false, confidential = false)
	public Integer getConsumerFetchMinBytes() {
		return consumerFetchMinBytes;
	}
	
	public void setConsumerFetchMinBytes(Integer consumerFetchMinBytes) {
		this.consumerFetchMinBytes = consumerFetchMinBytes;
	}
	
	@ConfigurationProperty(order = 45, displayMessageKey = "consumerFetchMaxBytes.display",
			helpMessageKey = "consumerFetchMaxBytes.help", required = false, confidential = false)
	public Integer getConsumerFetchMaxBytes() {
		return consumerFetchMaxBytes;
	}
	
	public void setConsumerFetchMaxBytes(Integer consumerFetchMaxBytes) {
		this.consumerFetchMaxBytes = consumerFetchMaxBytes;
	}
	
	@ConfigurationProperty(order = 46, displayMessageKey = "consumerFetchMaxWaitMs.display",
			helpMessageKey = "consumerFetchMaxWaitMs.help", required = false, confidential = false)
	public Integer getConsumerFetchMaxWaitMs() {
		return consumerFetchMaxWaitMs;
	}
	
	public void setConsumerFetchMaxWaitMs(Integer consumerFetchMaxWaitMs) {
		this.consumerFetchMaxWaitMs = consumerFetchMaxWaitMs;
	}
	
	@ConfigurationProperty(order = 47, displayMessageKey = "consumerReconnectBackoffMs.display",
			helpMessageKey = "consumerReconnectBackoffMs.help", required = false, confidential = false)
	public Long getConsumerReconnectBackoffMs() {
		return consumerReconnectBackoffMs;
	}
	
	public void setConsumerReconnectBackoffMs(Long consumerReconnectBackoffMs) {
		this.consumerReconnectBackoffMs = consumerReconnectBackoffMs;
	}
	
	@ConfigurationProperty(order = 48, displayMessageKey = "consumerReconnectBackoffMaxMs.display",
			helpMessageKey = "consumerReconnectBackoffMaxMs.help", required = false, confidential = false)
	public Long getConsumerReconnectBackoffMaxMs() {
		return consumerReconnectBackoffMaxMs;
	}
	
	public void setConsumerReconnectBackoffMaxMs(Long consumerReconnectBackoffMaxMs) {
		this.consumerReconnectBackoffMaxMs = consumerReconnectBackoffMaxMs;
	}
	
	@ConfigurationProperty(order = 49, displayMessageKey = "consumerRetryBackoffMs.display",
			helpMessageKey = "consumerRetryBackoffMs.help", required = false, confidential = false)
	public Long getConsumerRetryBackoffMs() {
		return consumerRetryBackoffMs;
	}
	
	public void setConsumerRetryBackoffMs(Long consumerRetryBackoffMs) {
		this.consumerRetryBackoffMs = consumerRetryBackoffMs;
	}
	
	@ConfigurationProperty(order = 50, displayMessageKey = "consumerAutoOffsetReset.display",
			helpMessageKey = "consumerAutoOffsetReset.help", required = false, confidential = false)
	public String getConsumerAutoOffsetReset() {
		return consumerAutoOffsetReset;
	}
	
	public void setConsumerAutoOffsetReset(String consumerAutoOffsetReset) {
		this.consumerAutoOffsetReset = consumerAutoOffsetReset;
	}
	
	@ConfigurationProperty(order = 51, displayMessageKey = "consumerCheckCrcs.display",
			helpMessageKey = "consumerCheckCrcs.help", required = false, confidential = false)
	public Boolean getConsumerCheckCrcs() {
		return consumerCheckCrcs;
	}
	
	public void setConsumerCheckCrcs(Boolean consumerCheckCrcs) {
		this.consumerCheckCrcs = consumerCheckCrcs;
	}
	
	@ConfigurationProperty(order = 52, displayMessageKey = "consumerRequestTimeoutMs.display",
			helpMessageKey = "consumerRequestTimeoutMs.help", required = false, confidential = false)
	public Integer getConsumerRequestTimeoutMs() {
		return consumerRequestTimeoutMs;
	}
	
	public void setConsumerRequestTimeoutMs(Integer consumerRequestTimeoutMs) {
		this.consumerRequestTimeoutMs = consumerRequestTimeoutMs;
	}
	
	@ConfigurationProperty(order = 53, displayMessageKey = "consumerConnectionsMaxIdleMs.display",
			helpMessageKey = "consumerConnectionsMaxIdleMs.help", required = false, confidential = false)
	public Long getConsumerConnectionsMaxIdleMs() {
		return consumerConnectionsMaxIdleMs;
	}
	
	public void setConsumerConnectionsMaxIdleMs(Long consumerConnectionsMaxIdleMs) {
		this.consumerConnectionsMaxIdleMs = consumerConnectionsMaxIdleMs;
	}
	
	@ConfigurationProperty(order = 54, displayMessageKey = "consumerMaxPollRecords.display",
			helpMessageKey = "consumerMaxPollRecords.help", required = false, confidential = false)
	public Integer getConsumerMaxPollRecords() {
		return consumerMaxPollRecords;
	}
	
	public void setConsumerMaxPollRecords(Integer consumerMaxPollRecords) {
		this.consumerMaxPollRecords = consumerMaxPollRecords;
	}
	
	@ConfigurationProperty(order = 55, displayMessageKey = "consumerMaxPollIntervalMs.display",
			helpMessageKey = "consumerMaxPollIntervalMs.help", required = false, confidential = false)
	public Integer getConsumerMaxPollIntervalMs() {
		return consumerMaxPollIntervalMs;
	}
	
	public void setConsumerMaxPollIntervalMs(Integer consumerMaxPollIntervalMs) {
		this.consumerMaxPollIntervalMs = consumerMaxPollIntervalMs;
	}
	
	@ConfigurationProperty(order = 56, displayMessageKey = "consumerExcludeInternalTopics.display",
			helpMessageKey = "consumerExcludeInternalTopics.help", required = false, confidential = false)
	public Boolean getConsumerExcludeInternalTopics() {
		return consumerExcludeInternalTopics;
	}
	
	public void setConsumerExcludeInternalTopics(Boolean consumerExcludeInternalTopics) {
		this.consumerExcludeInternalTopics = consumerExcludeInternalTopics;
	}
	
	@ConfigurationProperty(order = 58, displayMessageKey = "consumerIsolationLevel.display",
			helpMessageKey = "consumerIsolationLevel.help", required = false, confidential = false)
	public String getConsumerIsolationLevel() {
		return consumerIsolationLevel;
	}
	
	public void setConsumerIsolationLevel(String consumerIsolationLevel) {
		this.consumerIsolationLevel = consumerIsolationLevel;
	}
	
	@ConfigurationProperty(order = 59, displayMessageKey = "consumerSecurityProtocol.display",
			helpMessageKey = "consumerSecurityProtocol.help", required = false, confidential = false)
	public String getConsumerSecurityProtocol() {
		return consumerSecurityProtocol;
	}
	
	public void setConsumerSecurityProtocol(String consumerSecurityProtocol) {
		this.consumerSecurityProtocol = consumerSecurityProtocol;
	}
	
	@ConfigurationProperty(order = 60, displayMessageKey = "consumerSslProtocol.display",
			helpMessageKey = "consumerSslProtocol.help", required = false, confidential = false)
	public String getConsumerSslProtocol() {
		return consumerSslProtocol;
	}
	
	public void setConsumerSslProtocol(String consumerSslProtocol) {
		this.consumerSslProtocol = consumerSslProtocol;
	}
	
	@ConfigurationProperty(order = 61, displayMessageKey = "consumerSslProvider.display",
			helpMessageKey = "consumerSslProvider.help", required = false, confidential = false)
	public String getConsumerSslProvider() {
		return consumerSslProvider;
	}
	
	public void setConsumerSslProvider(String consumerSslProvider) {
		this.consumerSslProvider = consumerSslProvider;
	}
	
	@ConfigurationProperty(order = 62, displayMessageKey = "consumerSslCipherSuites.display",
			helpMessageKey = "consumerSslCipherSuites.help", required = false, confidential = false)
	public String getConsumerSslCipherSuites() {
		return consumerSslCipherSuites;
	}
	
	public void setConsumerSslCipherSuites(String consumerSslCipherSuites) {
		this.consumerSslCipherSuites = consumerSslCipherSuites;
	}
	
	@ConfigurationProperty(order = 63, displayMessageKey = "consumerSslEnabledProtocols.display",
			helpMessageKey = "consumerSslEnabledProtocols.help", required = false, confidential = false)
	public String getConsumerSslEnabledProtocols() {
		return consumerSslEnabledProtocols;
	}
	
	public void setConsumerSslEnabledProtocols(String consumerSslEnabledProtocols) {
		this.consumerSslEnabledProtocols = consumerSslEnabledProtocols;
	}
	
	@ConfigurationProperty(order = 64, displayMessageKey = "consumerSslEndpointIdentificationAlgorithm.display",
			helpMessageKey = "consumerSslEndpointIdentificationAlgorithm.help", required = false, confidential = false)
	public String getConsumerSslEndpointIdentificationAlgorithm() {
		return consumerSslEndpointIdentificationAlgorithm;
	}
	
	public void setConsumerSslEndpointIdentificationAlgorithm(String consumerSslEndpointIdentificationAlgorithm) {
		this.consumerSslEndpointIdentificationAlgorithm = consumerSslEndpointIdentificationAlgorithm;
	}
	
	@ConfigurationProperty(order = 65, displayMessageKey = "consumerSslSecureRandomImplementation.display",
			helpMessageKey = "consumerSslSecureRandomImplementation.help", required = false, confidential = false)
	public String getConsumerSslSecureRandomImplementation() {
		return consumerSslSecureRandomImplementation;
	}
	
	public void setConsumerSslSecureRandomImplementation(String consumerSslSecureRandomImplementation) {
		this.consumerSslSecureRandomImplementation = consumerSslSecureRandomImplementation;
	}
	
	@ConfigurationProperty(order = 66, displayMessageKey = "sslPrivateKeyEntryAlias.display",
			helpMessageKey = "sslPrivateKeyEntryAlias.help", required = false, confidential = false)
	public String getSslPrivateKeyEntryAlias() {
		return sslPrivateKeyEntryAlias;
	}
	
	public void setSslPrivateKeyEntryAlias(String sslPrivateKeyEntryAlias) {
		this.sslPrivateKeyEntryAlias = sslPrivateKeyEntryAlias;
	}
	
	@ConfigurationProperty(order = 67, displayMessageKey = "sslPrivateKeyEntryPassword.display",
			helpMessageKey = "sslPrivateKeyEntryPassword.help", required = false, confidential = false)
	public GuardedString getSslPrivateKeyEntryPassword() {
		return sslPrivateKeyEntryPassword;
	}
	
	public void setSslPrivateKeyEntryPassword(GuardedString sslPrivateKeyEntryPassword) {
		this.sslPrivateKeyEntryPassword = sslPrivateKeyEntryPassword;
	}
	
	@ConfigurationProperty(order = 68, displayMessageKey = "sslTrustCertificateAliasPrefix.display",
			helpMessageKey = "sslTrustCertificateAliasPrefix.help", required = false, confidential = false)
	public String getSslTrustCertificateAliasPrefix() {
		return sslTrustCertificateAliasPrefix;
	}
	
	public void setSslTrustCertificateAliasPrefix(String sslTrustCertificateAliasPrefix) {
		this.sslTrustCertificateAliasPrefix = sslTrustCertificateAliasPrefix;
	}
	
	@ConfigurationProperty(order = 70, displayMessageKey = "ssoUrlRenewal.display",
			helpMessageKey = "ssoUrlRenewal.help", required = false, confidential = false)
	public String getSsoUrlRenewal() {
		return ssoUrlRenewal;
	}
	
	public void setSsoUrlRenewal(String ssoUrlRenewal) {
		this.ssoUrlRenewal = ssoUrlRenewal;
	}
	
	@ConfigurationProperty(order = 71, displayMessageKey = "serviceUrlRenewal.display",
			helpMessageKey = "serviceUrlRenewal.help", required = false, confidential = false)
	public String getServiceUrlRenewal() {
		return serviceUrlRenewal;
	}
	
	public void setServiceUrlRenewal(String serviceUrlRenewal) {
		this.serviceUrlRenewal = serviceUrlRenewal;
	}
	
	@ConfigurationProperty(order = 72, displayMessageKey = "usernameRenewal.display",
			helpMessageKey = "usernameRenewal.help", required = false, confidential = false)
	public String getUsernameRenewal() {
		return usernameRenewal;
	}
	
	public void setUsernameRenewal(String usernameRenewal) {
		this.usernameRenewal = usernameRenewal;
	}
	
	@ConfigurationProperty(order = 73, displayMessageKey = "passwordRenewal.display",
			helpMessageKey = "passwordRenewal.help", required = false, confidential = false)
	public GuardedString getPasswordRenewal() {
		return passwordRenewal;
	}
	
	public void setPasswordRenewal(GuardedString passwordRenewal) {
		this.passwordRenewal = passwordRenewal;
	}
	
	@ConfigurationProperty(order = 74, displayMessageKey = "clientIdRenewal.display",
			helpMessageKey = "clientIdRenewal.help", required = false, confidential = false)
	public String getClientIdRenewal() {
		return clientIdRenewal;
	}
	
	public void setClientIdRenewal(String clientIdRenewal) {
		this.clientIdRenewal = clientIdRenewal;
	}
	
	@ConfigurationProperty(order = 75, displayMessageKey = "intervalForCertificateRenewal.display",
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
		this.partitionOfTopic = null;
		this.consumerSessionTimeoutMs = null;
		this.consumerHeartbeatIntervalMs = null;
		this.consumerPartitionAssignmentStrategy = null;
		this.consumerMetadataMaxAgeMs = null;
		this.consumerClientId = null;
		this.consumerMaxPartitionFetchBytes = null;
		this.consumerSendBufferBytes = null;
		this.consumerReceiveBufferBytes = null;
		this.consumerFetchMinBytes = null;
		this.consumerFetchMaxBytes = null;
		this.consumerFetchMaxWaitMs = null;
		this.consumerReconnectBackoffMs = null;
		this.consumerReconnectBackoffMaxMs = null;
		this.consumerRetryBackoffMs = null;
		this.consumerAutoOffsetReset = null;
		this.consumerCheckCrcs = null;
		this.consumerRequestTimeoutMs = null;
		this.consumerConnectionsMaxIdleMs = null;
		this.consumerMaxPollRecords = null;
		this.consumerMaxPollIntervalMs = null;
		this.consumerExcludeInternalTopics = null;
		this.consumerIsolationLevel = null;
		this.consumerSecurityProtocol = null;
		this.consumerSslProtocol = null;
		this.consumerSslProvider = null;
		this.consumerSslCipherSuites = null;
		this.consumerSslEnabledProtocols = null;
		this.consumerSslEndpointIdentificationAlgorithm = null;
		this.consumerSslSecureRandomImplementation = null;
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
				.append(", partitionOfTopic='").append(partitionOfTopic).append("'");
		writeOptionalProperties(sb, "nameAttribute", nameAttribute);
		writeOptionalProperties(sb, "schemaRegistryClassLoaderCacheSize", schemaRegistryClassLoaderCacheSize);
		writeOptionalProperties(sb, "schemaRegistryClassLoaderCacheExpiryInterval", schemaRegistryClassLoaderCacheExpiryInterval);
		writeOptionalProperties(sb, "schemaRegistrySchemaVersionCacheSize", schemaRegistrySchemaVersionCacheSize);
		writeOptionalProperties(sb, "schemaRegistrySchemaVersionCacheExpiryInterval", schemaRegistrySchemaVersionCacheExpiryInterval);
		writeOptionalProperties(sb, "schemaRegistrySchemaMetadataCacheExpiryInterval", schemaRegistrySchemaMetadataCacheExpiryInterval);
		writeOptionalProperties(sb, "schemaRegistrySchemaTextCacheSize", schemaRegistrySchemaTextCacheSize);
		writeOptionalProperties(sb, "schemaRegistrySchemaTextCacheExpiryInterval", schemaRegistrySchemaTextCacheExpiryInterval);
		writeOptionalProperties(sb, "schemaRegistrySslProtocol", schemaRegistrySslProtocol);
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
		writeOptionalProperties(sb, "consumerSessionTimeoutMs", consumerSessionTimeoutMs);
		writeOptionalProperties(sb, "consumerHeartbeatIntervalMs", consumerHeartbeatIntervalMs);
		writeOptionalProperties(sb, "consumerPartitionAssignmentStrategy", consumerPartitionAssignmentStrategy);
		writeOptionalProperties(sb, "consumerMetadataMaxAgeMs", consumerMetadataMaxAgeMs);
		writeOptionalProperties(sb, "consumerClientId", consumerClientId);
		writeOptionalProperties(sb, "consumerMaxPartitionFetchBytes", consumerMaxPartitionFetchBytes);
		writeOptionalProperties(sb, "consumerSendBufferBytes", consumerSendBufferBytes);
		writeOptionalProperties(sb, "consumerReceiveBufferBytes", consumerReceiveBufferBytes);
		writeOptionalProperties(sb, "consumerFetchMinBytes", consumerFetchMinBytes);
		writeOptionalProperties(sb, "consumerFetchMaxBytes", consumerFetchMaxBytes);
		writeOptionalProperties(sb, "consumerFetchMaxWaitMs", consumerFetchMaxWaitMs);
		writeOptionalProperties(sb, "consumerReconnectBackoffMs", consumerReconnectBackoffMs);
		writeOptionalProperties(sb, "consumerReconnectBackoffMaxMs", consumerReconnectBackoffMaxMs);
		writeOptionalProperties(sb, "consumerRetryBackoffMs", consumerRetryBackoffMs);
		writeOptionalProperties(sb, "consumerAutoOffsetReset", consumerAutoOffsetReset);
		writeOptionalProperties(sb, "consumerCheckCrcs", consumerCheckCrcs);
		writeOptionalProperties(sb, "consumerRequestTimeoutMs", consumerRequestTimeoutMs);
		writeOptionalProperties(sb, "consumerConnectionsMaxIdleMs", consumerConnectionsMaxIdleMs);
		writeOptionalProperties(sb, "consumerMaxPollRecords", consumerMaxPollRecords);
		writeOptionalProperties(sb, "consumerMaxPollIntervalMs", consumerMaxPollIntervalMs);
		writeOptionalProperties(sb, "consumerExcludeInternalTopics", consumerExcludeInternalTopics);
		writeOptionalProperties(sb, "consumerIsolationLevel", consumerIsolationLevel);
		writeOptionalProperties(sb, "consumerSecurityProtocol", consumerSecurityProtocol);
		writeOptionalProperties(sb, "consumerSslProtocol", consumerSslProtocol);
		writeOptionalProperties(sb, "consumerSslProvider", consumerSslProvider);
		writeOptionalProperties(sb, "consumerSslEnabledProtocols", consumerSslEnabledProtocols);
		writeOptionalProperties(sb, "consumerSslEndpointIdentificationAlgorithm", consumerSslEndpointIdentificationAlgorithm);
		writeOptionalProperties(sb, "consumerSslSecureRandomImplementation", consumerSslSecureRandomImplementation);
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