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
	
	protected KafkaConfiguration getConfiguration(){
		KafkaConfiguration conf = new KafkaConfiguration();
		conf.setConsumerBootstrapServers(parser.getBootstrapServers());
		conf.setSchemaRegistryUrl(parser.getSchemaRegistryUrl());
		conf.setNameOfSchema(parser.getNameOfSchema());
		conf.setVersionOfSchema(parser.getVersionOfSchema());
		conf.setConsumerNameOfTopic(parser.getNameOfTopic());
		conf.setUniqueAttribute(parser.getUniqueAttribute());
		conf.setNameAttribute(parser.getNameAttribute());
		conf.setSchemaRegistrySslProtocol(parser.getSslProtocol());
		conf.setSslKeyStorePath(parser.getSslKeyStorePath());
		conf.setSslKeyStorePassword(parser.getSslKeyStorePassword());
		conf.setSslTrustStorePath(parser.getSslTrustStorePath());
		conf.setSslTrustStorePassword(parser.getSslTrustStorePassword());
		conf.setConsumerGroupId(parser.getGroupId());
		conf.setSslPrivateKeyEntryAlias(parser.getPrivateKeyAlias());
		conf.setSslPrivateKeyEntryPassword(parser.getPrivateKeyPassword());
		conf.setSslTrustCertificateAliasPrefix(parser.getCertificateAliasPrefix());
		conf.setSsoUrlRenewal(parser.getTokenUrl());
		conf.setServiceUrlRenewal(parser.getServiceUrl());
		conf.setUsernameRenewal(parser.getUsername());
		conf.setPasswordRenewal(parser.getPassword());
		conf.setClientIdRenewal(parser.getClintId());
		conf.setConsumerPartitionOfTopic(parser.getPartitions());
		
		return conf;
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
