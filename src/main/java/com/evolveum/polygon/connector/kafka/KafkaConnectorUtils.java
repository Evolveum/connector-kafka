package com.evolveum.polygon.connector.kafka;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.identityconnectors.common.StringUtil;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.common.security.GuardedString.Accessor;

import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import com.hortonworks.registries.schemaregistry.serdes.avro.AbstractAvroSnapshotDeserializer;
import com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroDeserializer;

public class KafkaConnectorUtils {
	
	private static final String SCHEMA_REGISTRY_CLIENT_SSL_KEY ="schema.registry.client.ssl";
	private static final String PROTOCOL_KEY ="protocol";
	private static final String KEY_STORE_TYPE_KEY ="keyStoreType";
	private static final String KEY_STORE_PATH_KEY ="keyStorePath";
	private static final String KEY_STORE_PASSWORD_KEY ="keyStorePassword";
	private static final String KEY_STORE_PROVIDER_KEY ="keyStoreProvider";
	private static final String KEY_PASSWORD_KEY ="keyPassword";
	private static final String KEY_STORE_MANAGER_FACTORY_PROVIDER_KEY ="keyManagerFactoryProvider";
	private static final String KEY_STORE_MANAGER_FACTORY_ALGORITHM_KEY ="keyManagerFactoryAlgorithm";
	private static final String TRUST_STORE_TYPE_KEY ="trustStoreType";
	private static final String TRUST_STORE_PATH_KEY ="trustStorePath";
	private static final String TRUST_STORE_PASSWORD_KEY ="trustStorePassword";
	private static final String TRUST_STORE_PROVIDER_KEY ="trustStoreProvider";
	private static final String TRUST_STORE_MANAGER_FACTORY_PROVIDER_KEY ="trustManagerFactoryProvider";
	private static final String TRUST_STORE_MANAGER_FACTORY_ALGORITHM_KEY ="trustManagerFactoryAlgorithm";

	public static Map<String, String> getSchemaRegistrySslProperties(KafkaConfiguration configuration){
		
		Map<String, String> sslConfig = new HashMap<String, String>();
		writeOptionalProperties(sslConfig, PROTOCOL_KEY, configuration.getSchemaRegistrySslProtocol());
		writeOptionalProperties(sslConfig, KEY_STORE_TYPE_KEY, configuration.getSslKeyStoreType());
		writeOptionalProperties(sslConfig, KEY_STORE_PATH_KEY, configuration.getSslKeyStorePath());
		writeOptionalProperties(sslConfig, KEY_STORE_PASSWORD_KEY, configuration.getSslKeyStorePassword());
		writeOptionalProperties(sslConfig, KEY_PASSWORD_KEY, configuration.getSslKeyPassword());
		writeOptionalProperties(sslConfig, KEY_STORE_PROVIDER_KEY, configuration.getSslKeyStoreProvider());
		writeOptionalProperties(sslConfig, KEY_STORE_MANAGER_FACTORY_PROVIDER_KEY, configuration.getSslKeyManagerFactoryProvider());
		writeOptionalProperties(sslConfig, KEY_STORE_MANAGER_FACTORY_ALGORITHM_KEY, configuration.getSslKeyManagerFactoryAlgorithm());
		writeOptionalProperties(sslConfig, TRUST_STORE_TYPE_KEY, configuration.getSslTrustStoreType());
		writeOptionalProperties(sslConfig, TRUST_STORE_PATH_KEY, configuration.getSslTrustStorePath());
		writeOptionalProperties(sslConfig, TRUST_STORE_PASSWORD_KEY, configuration.getSslTrustStorePassword());
		writeOptionalProperties(sslConfig, TRUST_STORE_PROVIDER_KEY, configuration.getSslTrustStoreProvider());
		writeOptionalProperties(sslConfig, TRUST_STORE_MANAGER_FACTORY_PROVIDER_KEY, configuration.getSslTrustManagerFactoryProvider());
		writeOptionalProperties(sslConfig, TRUST_STORE_MANAGER_FACTORY_ALGORITHM_KEY, configuration.getSslTrustManagerFactoryAlgorithm());
		return sslConfig;
	}
	
	public static Map<String, Object> getSchemaRegistryConfigProperties(KafkaConfiguration configuration) {
		Map<String, Object> config = new HashMap<String, Object>();
		config.put(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(), configuration.getSchemaRegistryUrl());
		writeOptionalProperties(config, SchemaRegistryClient.Configuration.CLASSLOADER_CACHE_SIZE.name(), configuration.getSchemaRegistryClassLoaderCacheSize());
		writeOptionalProperties(config, SchemaRegistryClient.Configuration.CLASSLOADER_CACHE_EXPIRY_INTERVAL_SECS.name(), configuration.getSchemaRegistryClassLoaderCacheExpiryInterval());
		writeOptionalProperties(config, SchemaRegistryClient.Configuration.SCHEMA_VERSION_CACHE_SIZE.name(), configuration.getSchemaRegistrySchemaVersionCacheSize());
		writeOptionalProperties(config, SchemaRegistryClient.Configuration.SCHEMA_VERSION_CACHE_EXPIRY_INTERVAL_SECS.name(), configuration.getSchemaRegistrySchemaVersionCacheExpiryInterval());
		writeOptionalProperties(config, SchemaRegistryClient.Configuration.SCHEMA_METADATA_CACHE_SIZE.name(), configuration.getSchemaRegistrySchemaMetadataCacheSize());
		writeOptionalProperties(config, SchemaRegistryClient.Configuration.SCHEMA_METADATA_CACHE_EXPIRY_INTERVAL_SECS.name(), configuration.getSchemaRegistrySchemaMetadataCacheExpiryInterval());
		writeOptionalProperties(config, SchemaRegistryClient.Configuration.SCHEMA_TEXT_CACHE_SIZE.name(), configuration.getSchemaRegistrySchemaTextCacheSize());
		writeOptionalProperties(config, SchemaRegistryClient.Configuration.SCHEMA_TEXT_CACHE_EXPIRY_INTERVAL_SECS.name(), configuration.getSchemaRegistrySchemaTextCacheExpiryInterval());
		
		Map<String, String> sslConfig = getSchemaRegistrySslProperties(configuration);
		if(!sslConfig.isEmpty()) {
			config.put(SCHEMA_REGISTRY_CLIENT_SSL_KEY, sslConfig);
		}
		return config;
	}
	
	public static Properties getConsumerProperties(KafkaConfiguration configuration) {
		Properties properties = new Properties();
	    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, configuration.getConsumerBootstrapServers());
	    properties.put(ConsumerConfig.GROUP_ID_CONFIG, configuration.getConsumerGroupId());
	    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
	    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
	    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
	    properties.put(AbstractAvroSnapshotDeserializer.SPECIFIC_AVRO_READER, false);
	    properties.put("serdes.protocol.version", 0);
	    writeOptionalProperties(properties, ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, configuration.getConsumerSessionTimeoutMs());
	    writeOptionalProperties(properties, ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, configuration.getConsumerHeartbeatIntervalMs());
	    writeOptionalProperties(properties, ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, configuration.getConsumerPartitionAssignmentStrategy());
	    writeOptionalProperties(properties, ConsumerConfig.METADATA_MAX_AGE_CONFIG, configuration.getConsumerMetadataMaxAgeMs());
	    writeOptionalProperties(properties, ConsumerConfig.CLIENT_ID_CONFIG, configuration.getConsumerClientId());
	    writeOptionalProperties(properties, ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, configuration.getConsumerMaxPartitionFetchBytes());
	    writeOptionalProperties(properties, ConsumerConfig.SEND_BUFFER_CONFIG, configuration.getConsumerSendBufferBytes());
	    writeOptionalProperties(properties, ConsumerConfig.RECEIVE_BUFFER_CONFIG, configuration.getConsumerReceiveBufferBytes());
	    writeOptionalProperties(properties, ConsumerConfig.FETCH_MIN_BYTES_CONFIG, configuration.getConsumerFetchMinBytes());
	    writeOptionalProperties(properties, ConsumerConfig.FETCH_MAX_BYTES_CONFIG, configuration.getConsumerFetchMaxBytes());
	    writeOptionalProperties(properties, ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, configuration.getConsumerFetchMaxWaitMs());
	    writeOptionalProperties(properties, ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, configuration.getConsumerReconnectBackoffMs());
	    writeOptionalProperties(properties, ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, configuration.getConsumerReconnectBackoffMaxMs());
	    writeOptionalProperties(properties, ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, configuration.getConsumerRetryBackoffMs());
	    writeOptionalProperties(properties, ConsumerConfig.CHECK_CRCS_CONFIG, configuration.getConsumerCheckCrcs());
	    writeOptionalProperties(properties, ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, configuration.getConsumerRequestTimeoutMs());
	    writeOptionalProperties(properties, ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, configuration.getConsumerConnectionsMaxIdleMs());
	    writeOptionalProperties(properties, ConsumerConfig.MAX_POLL_RECORDS_CONFIG, configuration.getConsumerMaxPollRecords());
	    writeOptionalProperties(properties, ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, configuration.getConsumerMaxPollIntervalMs());
	    writeOptionalProperties(properties, ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG, configuration.getConsumerExcludeInternalTopics());
	    writeOptionalProperties(properties, ConsumerConfig.ISOLATION_LEVEL_CONFIG, configuration.getConsumerIsolationLevel());
	    writeOptionalProperties(properties, CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, configuration.getConsumerSecurityProtocol());
	    writeOptionalProperties(properties, SslConfigs.SSL_PROTOCOL_CONFIG, configuration.getConsumerSslProtocol());
	    writeOptionalProperties(properties, SslConfigs.SSL_PROVIDER_CONFIG, configuration.getConsumerSslProvider());
	    writeOptionalProperties(properties, SslConfigs.SSL_CIPHER_SUITES_CONFIG, configuration.getConsumerSslCipherSuites());
	    writeOptionalProperties(properties, SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, configuration.getConsumerSslEnabledProtocols());
	    writeOptionalProperties(properties, SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, configuration.getSslKeyStoreType());
	    writeOptionalProperties(properties, SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, configuration.getSslTrustStoreType());
	    writeOptionalProperties(properties, SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, configuration.getSslKeyStorePath());
	    writeOptionalProperties(properties, SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, configuration.getSslKeyStorePassword());
	    writeOptionalProperties(properties, SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, configuration.getSslTrustStorePath());
	    writeOptionalProperties(properties, SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, configuration.getSslTrustStorePassword());
	    writeOptionalProperties(properties, SslConfigs.SSL_KEY_PASSWORD_CONFIG, configuration.getSslKeyPassword());
	    writeOptionalProperties(properties, SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG, configuration.getSslKeyManagerFactoryAlgorithm());
	    writeOptionalProperties(properties, SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG, configuration.getSslTrustManagerFactoryAlgorithm());
	    writeOptionalProperties(properties, SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, configuration.getConsumerSslEndpointIdentificationAlgorithm());
	    writeOptionalProperties(properties, SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG, configuration.getConsumerSslSecureRandomImplementation());
	    properties.putAll(getSchemaRegistryConfigProperties(configuration));
	    return properties;
	}
	
	private static void writeOptionalProperties(Properties properties, String name, Object value) {
		if(value != null) {
			properties.put(name, value);
		}
	}
	
	private static void writeOptionalProperties(Properties properties, String name, String value) {
		if(StringUtil.isNotBlank(value)) {
			properties.put(name, value);
		}
	}
	
	private static void writeOptionalProperties(Properties properties, String name, GuardedString value) {
		final StringBuilder password = new StringBuilder();
		if (value != null) {
			Accessor accessor = new GuardedString.Accessor() {
				@Override
				public void access(char[] chars) {
					password.append(new String(chars));
				}
			};
			value.access(accessor);
			properties.put(name, password.toString());
		}
	}
	
	private static void writeOptionalProperties(Map properties, String name, Object value) {
		if(value != null) {
			properties.put(name, value);
		}
	}
	
	private static void writeOptionalProperties(Map properties, String name, String value) {
		if(StringUtil.isNotBlank(value)) {
			properties.put(name, value);
		}
	}
	
	private static void writeOptionalProperties(Map properties, String name, GuardedString value) {
		final StringBuilder password = new StringBuilder();
		if (value != null) {
			Accessor accessor = new GuardedString.Accessor() {
				@Override
				public void access(char[] chars) {
					password.append(new String(chars));
				}
			};
			value.access(accessor);
			properties.put(name, password.toString());
		}
	}
	
	public static SchemaVersionInfo getSchemaVersionInfo(KafkaConfiguration configuration, SchemaRegistryClient client) throws SchemaNotFoundException {
		String schemaName = configuration.getNameOfSchema();
		Integer schemaVersion = configuration.getVersionOfSchema();
		SchemaVersionKey schemaVersionKey = new SchemaVersionKey(schemaName, schemaVersion);
		SchemaVersionInfo schemaVersionInfo = client.getSchemaVersionInfo(schemaVersionKey);
		return schemaVersionInfo;
	}
	
}
