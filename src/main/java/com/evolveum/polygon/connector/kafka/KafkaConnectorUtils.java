package com.evolveum.polygon.connector.kafka;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.identityconnectors.common.StringUtil;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.common.security.GuardedString.Accessor;

import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import com.hortonworks.registries.schemaregistry.serdes.avro.AbstractAvroSnapshotDeserializer;
import com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroDeserializer;

public class KafkaConnectorUtils {
	
	private static final Log LOGGER = Log.getLog(KafkaConnector.class);
	
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
		String pathToMorePropertiesForConsumer = configuration.getPathToMorePropertiesForConsumer();
		if(pathToMorePropertiesForConsumer != null) {
			try (InputStream input = new FileInputStream(pathToMorePropertiesForConsumer)) {
				// load a properties file
				properties.load(input);
			} catch (IOException e) {
				LOGGER.error(e, "Couldnt load file from path {0}", pathToMorePropertiesForConsumer);
			}
		}
		if(properties.contains(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
			properties.remove(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);
		}
	    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, configuration.getConsumerBootstrapServers());
	    
	    if(properties.contains(ConsumerConfig.GROUP_ID_CONFIG)) {
			properties.remove(ConsumerConfig.GROUP_ID_CONFIG);
		}
	    properties.put(ConsumerConfig.GROUP_ID_CONFIG, configuration.getConsumerGroupId());
	    
	    if(properties.contains(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)) {
			properties.remove(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
		}
	    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
	    
	    if(properties.contains(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG)) {
			properties.remove(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
		}
	    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
	    
	    if(!properties.contains(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)) {
	    	properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		}
	    
	    if(properties.contains(AbstractAvroSnapshotDeserializer.SPECIFIC_AVRO_READER)) {
			properties.remove(AbstractAvroSnapshotDeserializer.SPECIFIC_AVRO_READER);
		}
	    properties.put(AbstractAvroSnapshotDeserializer.SPECIFIC_AVRO_READER, false);
	    
	    if(properties.contains("serdes.protocol.version")) {
			properties.remove("serdes.protocol.version");
		}
	    properties.put("serdes.protocol.version", 0);
	    
	    if(properties.contains(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG)) {
	    	if(configuration.getConsumerSecurityProtocol() != null) {
	    		properties.remove(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG);
	    		writeOptionalProperties(properties, CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, configuration.getConsumerSecurityProtocol());
	    	}
	    } else {
	    	writeOptionalProperties(properties, CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, configuration.getConsumerSecurityProtocol());
	    }
	    
	    if(properties.contains(ConsumerConfig.MAX_POLL_RECORDS_CONFIG)) {
	    	if(configuration.getConsumerSecurityProtocol() != null) {
	    		properties.remove(ConsumerConfig.MAX_POLL_RECORDS_CONFIG);
	    		writeOptionalProperties(properties, ConsumerConfig.MAX_POLL_RECORDS_CONFIG, configuration.getConsumerMaxRecords());
	    	}
	    } else {
	    	writeOptionalProperties(properties, ConsumerConfig.MAX_POLL_RECORDS_CONFIG, configuration.getConsumerMaxRecords());
	    }
	    
	    writeOptionalProperties(properties, SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, configuration.getSslKeyStoreType());
	    writeOptionalProperties(properties, SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, configuration.getSslTrustStoreType());
	    writeOptionalProperties(properties, SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, configuration.getSslKeyStorePath());
	    writeOptionalProperties(properties, SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, configuration.getSslKeyStorePassword());
	    writeOptionalProperties(properties, SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, configuration.getSslTrustStorePath());
	    writeOptionalProperties(properties, SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, configuration.getSslTrustStorePassword());
	    writeOptionalProperties(properties, SslConfigs.SSL_KEY_PASSWORD_CONFIG, configuration.getSslKeyPassword());
	    writeOptionalProperties(properties, SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG, configuration.getSslKeyManagerFactoryAlgorithm());
	    writeOptionalProperties(properties, SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG, configuration.getSslTrustManagerFactoryAlgorithm());
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
	
	public static List<TopicPartition> getPatritions(KafkaConfiguration configuration) {
		
		List<TopicPartition> topicPartitions = new ArrayList<TopicPartition>();
		String partitions = configuration.getConsumerPartitionOfTopic();
		partitions.replaceAll("\\s","");
		for(String partition: partitions.split(",")) {
			if(StringUtil.isBlank(partition)) {
				continue;
			}
			if(partition.contains("-")) {
				String[] numbers = partition.split("-");
				if(numbers.length != 2) {
					throw new IllegalArgumentException("Range for partition have to containts two number");
				}
				int start = Integer.parseInt(numbers[0]);
				int end = Integer.parseInt(numbers[1]);
				while(start <= end) {
					TopicPartition topicPartition = new TopicPartition(configuration.getConsumerNameOfTopic(), start);
					topicPartitions.add(topicPartition);
					start++;
				}
			} else {
				TopicPartition topicPartition = new TopicPartition(configuration.getConsumerNameOfTopic(), Integer.parseInt(partition));
				topicPartitions.add(topicPartition);
			}
		}
		return topicPartitions;
	}
	
	public static void addNewOffset(long offset, int partition, StringBuilder sb) {
		sb.append("P").append(partition).append("-")
		.append(offset);
	}

	public static String parseToken(Map<Integer, Long> newOffsets) {
		StringBuilder sb = new StringBuilder();
		int i = 1;
		for (int partition : newOffsets.keySet()) {
			KafkaConnectorUtils.addNewOffset(newOffsets.get(partition), partition, sb);
			if(i !=  newOffsets.size()) {
				sb.append(";");
			}
			i++;
		}
		return sb.toString();
	}
}
