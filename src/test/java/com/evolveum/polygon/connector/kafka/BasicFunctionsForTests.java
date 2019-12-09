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

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSession;


/**
 * @author skublik
 */
public class BasicFunctionsForTests {
	
	private PropertiesParser parser = new PropertiesParser();

	protected KafkaConfiguration getConsumerConfiguration(){
		return getConsumerConfiguration(false);
	}

	protected KafkaConfiguration getConsumerAndProducerConfiguration(){
		return getConsumerAndProducerConfiguration(false);
	}

	protected KafkaConfiguration getConsumerAndProducerConfiguration(boolean useSsl){
		KafkaConfiguration conf = new KafkaConfiguration();
		configureCommonProperties(conf, useSsl);
		conf.setConsumerNameOfTopic(parser.getConsumerNameOfTopic());
		conf.setConsumerVersionOfSchema(parser.getVersionOfSchema());
		conf.setConsumerPartitionOfTopic(parser.getPartitions());
		conf.setConsumerGroupId(parser.getGroupId());
		conf.setProducerNameOfTopic(parser.getProducerNameOfTopic());
		conf.setProducerPathToFileContainingSchema(parser.getPathForProducerSchema());
		conf.setUseOfConnector("CONSUMER_AND_PRODUCER");
		return conf;
	}
	
	protected KafkaConfiguration getConsumerConfiguration(boolean useSsl){
		KafkaConfiguration conf = new KafkaConfiguration();
		configureCommonProperties(conf, useSsl);
		conf.setConsumerNameOfTopic(parser.getConsumerNameOfTopic());
		conf.setConsumerVersionOfSchema(parser.getVersionOfSchema());
		conf.setConsumerPartitionOfTopic(parser.getPartitions());
		conf.setConsumerGroupId(parser.getGroupId());
		conf.setUseOfConnector("CONSUMER");
		return conf;
	}

	protected KafkaConfiguration getProducerConfiguration(){
		return getProducerConfiguration(false);
	}

	protected KafkaConfiguration getProducerConfiguration(boolean useSsl){
		KafkaConfiguration conf = new KafkaConfiguration();
		configureCommonProperties(conf, useSsl);
		conf.setProducerNameOfTopic(parser.getProducerNameOfTopic());
		conf.setProducerPathToFileContainingSchema(parser.getPathForProducerSchema());
		conf.setUseOfConnector("PRODUCER");
		return conf;
	}

	private void configureCommonProperties(KafkaConfiguration conf, boolean useSsl) {
		conf.setBootstrapServers(parser.getBootstrapServers());
		conf.setPasswordAttribute(parser.getPasswordAttribute());
		conf.setSchemaRegistryUrl(parser.getSchemaRegistryUrl());
		conf.setUniqueAttribute(parser.getUniqueAttribute());
		conf.setNameAttribute(parser.getNameAttribute());
		conf.setNameOfSchema(parser.getNameOfSchema());
		if (useSsl) {
			conf.setSchemaRegistrySslProtocol(parser.getSslProtocol());
			conf.setSslKeyStorePath(parser.getSslKeyStorePath());
			conf.setSslKeyStorePassword(parser.getSslKeyStorePassword());
			conf.setSslTrustStorePath(parser.getSslTrustStorePath());
			conf.setSslTrustStorePassword(parser.getSslTrustStorePassword());
			conf.setSslPrivateKeyEntryAlias(parser.getPrivateKeyAlias());
			conf.setSslPrivateKeyEntryPassword(parser.getPrivateKeyPassword());
			conf.setSslTrustCertificateAliasPrefix(parser.getCertificateAliasPrefix());
			conf.setSsoUrlRenewal(parser.getTokenUrl());
			conf.setServiceUrlRenewal(parser.getServiceUrl());
			conf.setUsernameRenewal(parser.getUsername());
			conf.setPasswordRenewal(parser.getPassword());
			conf.setClientIdRenewal(parser.getClintId());
		}
	}
	
	protected static void disableSslVerification() {
		HostnameVerifier allHostsValid = new HostnameVerifier() {
		    public boolean verify(String hostname, SSLSession session) {
		        return true;
		    }
		};
		HttpsURLConnection.setDefaultHostnameVerifier(allHostsValid);
	}
}
