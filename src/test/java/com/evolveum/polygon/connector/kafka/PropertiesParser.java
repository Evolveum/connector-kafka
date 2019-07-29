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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import org.identityconnectors.common.logging.Log;
import org.identityconnectors.common.security.GuardedString;

/**
 * @author skublik
 *
 */
public class PropertiesParser {

	private static final Log LOGGER = Log.getLog(PropertiesParser.class);
	private Properties properties;
	private String fileName = "propertiesForTests.properties";
	private static final String BOOTSTRAP_SERVER_HOST = "bootstrap.server";
	private static final String SCHEMA_REGISTRY_URL = "schema.registry.url";
	private static final String NAME_OF_SCHEMA = "schema.name";
	private static final String VERSION_OF_SCHEMA = "schema.version";
	private static final String NAME_OF_TOPIC = "topic.name";
	private static final String UNIQUE_ATTR = "attribute.unique";
	private static final String NAME_ATTR = "attribute.name";
	private static final String PROTOCOL = "schema.registry.ssl.protocol";
	private static final String TRUST_STORE_PATH = "schema.registry.ssl.trust.store.path";
	private static final String TRUST_STORE_PASSWORD = "schema.registry.ssl.trust.store.password";
	private static final String KEY_STORE_PATH = "schema.registry.ssl.key.store.path";
	private static final String KEY_STORE_PASSWORD = "schema.registry.ssl.key.store.password";
	private static final String CONSUMER_GROUP_ID_CONFIG = "consumer.group.id";
	private static final String PRIVATE_KEY_ALIAS =  "ssl.private.key.alias";
	private static final String PRIVATE_KEY_PASSWORD =  "ssl.private.key.password";
	private static final String CERTIFICATE_ALIAS =  "ssl.certificate.alias";
	private static final String TOKEN_URL = "token.url.renewal";
	private static final String SERVICE_URL = "service.url.renewal";
	private static final String USERNAME = "username.renewal";
	private static final String PASSWORD = "password.renewal";
	private static final String CLINT_ID = "client.id.renewal";
	
	public PropertiesParser() {

		try {
//			InputStreamReader fileInputStream = new InputStreamReader(new FileInputStream(filePath),
//					StandardCharsets.UTF_8);
			ClassLoader classLoader = getClass().getClassLoader();
			properties = new Properties();
			properties.load(classLoader.getResourceAsStream(fileName));
		} catch (FileNotFoundException e) {
			LOGGER.error("File not found: {0}", e.getLocalizedMessage());
			e.printStackTrace();
		} catch (IOException e) {
			LOGGER.error("IO exception occurred {0}", e.getLocalizedMessage());
			e.printStackTrace();
		}
	}

	public String getBootstrapServers(){
		return (String)properties.get(BOOTSTRAP_SERVER_HOST);
	}
	
	public String getSchemaRegistryUrl(){
		return (String)properties.get(SCHEMA_REGISTRY_URL);
	}
	
	public String getNameOfSchema(){
		return (String)properties.get(NAME_OF_SCHEMA);
	}
	
	public Integer getVersionOfSchema(){
		return Integer.valueOf((String)properties.get(VERSION_OF_SCHEMA));
	}
	
	public String getNameOfTopic(){
		return (String)properties.get(NAME_OF_TOPIC);
	}
	
	public String getUniqueAttribute(){
		return (String)properties.get(UNIQUE_ATTR);
	}
	
	public String getNameAttribute(){
		return (String)properties.get(NAME_ATTR);
	}
	
	public String getSslProtocol(){
		return (String)properties.get(PROTOCOL);
	}
	
	public String getSslTrustStorePath(){
		return (String)properties.get(TRUST_STORE_PATH);
	}
	
	public GuardedString getSslTrustStorePassword(){
		return new GuardedString(((String)properties.get(TRUST_STORE_PASSWORD)).toCharArray());
	}
	
	public String getSslKeyStorePath(){
		return (String)properties.get(KEY_STORE_PATH);
	}
	
	public GuardedString getSslKeyStorePassword(){
		return new GuardedString(((String)properties.get(KEY_STORE_PASSWORD)).toCharArray());
	}
	
	public String getGroupId(){
		return (String)properties.get(CONSUMER_GROUP_ID_CONFIG);
	}
	
	public String getPrivateKeyAlias() {
		return (String)properties.get(PRIVATE_KEY_ALIAS);
	}
	
	public GuardedString getPrivateKeyPassword() {
		return new GuardedString(((String)properties.get(PRIVATE_KEY_PASSWORD)).toCharArray());
	}
	
	public String getCertificateAlias() {
		return (String)properties.get(CERTIFICATE_ALIAS);
	}
	
	public String getTokenUrl() {
		return (String)properties.get(TOKEN_URL);
	}
	
	public String getServiceUrl() {
		return (String)properties.get(SERVICE_URL);
	}
	
	public String getUsername() {
		return (String)properties.get(USERNAME);
	}
	
	public GuardedString getPassword() {
		return new GuardedString(((String)properties.get(PASSWORD)).toCharArray());
	}
	
	public String getClintId() {
		return (String)properties.get(CLINT_ID);
	}
}