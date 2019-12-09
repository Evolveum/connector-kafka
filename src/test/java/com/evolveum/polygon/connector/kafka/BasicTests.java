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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.identityconnectors.common.logging.Log;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.common.objects.*;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author skublik
 */
public class BasicTests extends BasicFunctionsForTests {

	private static final Log LOGGER = Log.getLog(BasicTests.class);
	
	@Test(priority = 1)
	public void schemaTest(){
		KafkaConnector kafkaConnector = new KafkaConnector();
		disableSslVerification();
		KafkaConfiguration conf = getProducerConfiguration();
		kafkaConnector.init(conf);
		
		Schema schema = kafkaConnector.schema();
		LOGGER.info("Schema : {0}", schema.toString());
		conf.release();
		kafkaConnector.dispose();

		Set<AttributeInfo> attributesInfo = schema.findObjectClassInfo("test_schema").getAttributeInfo();
		for (AttributeInfo attrInfo : attributesInfo) {
			if (attrInfo.is(Uid.NAME) && attrInfo.getNativeName().equals("username") && attrInfo.isCreateable()
					&& attrInfo.isUpdateable() && attrInfo.isReadable() && attrInfo.isRequired() && !attrInfo.isMultiValued()
					&& attrInfo.getType().equals(String.class)) {
				continue;
			}
			if (attrInfo.is(Name.NAME) && attrInfo.getNativeName().equals("username") && attrInfo.isCreateable()
					&& attrInfo.isUpdateable() && attrInfo.isReadable() && attrInfo.isRequired() && !attrInfo.isMultiValued()
					&& attrInfo.getType().equals(String.class)) {
				continue;
			}
			if (attrInfo.is(OperationalAttributes.PASSWORD_NAME) && attrInfo.getNativeName().equals("password") && attrInfo.isCreateable()
					&& attrInfo.isUpdateable() && attrInfo.isReadable() && attrInfo.isRequired() && !attrInfo.isMultiValued()
					&& attrInfo.getType().equals(GuardedString.class)) {
				continue;
			}
			if (attrInfo.is("test_record.name") && attrInfo.getNativeName().equals("test_record.name") && attrInfo.isCreateable()
					&& attrInfo.isUpdateable() && attrInfo.isReadable() && !attrInfo.isRequired() && !attrInfo.isMultiValued()
					&& attrInfo.getType().equals(String.class)) {
				continue;
			}
			if (attrInfo.is("test_array") && attrInfo.getNativeName().equals("test_array") && attrInfo.isCreateable()
					&& attrInfo.isUpdateable() && attrInfo.isReadable() && !attrInfo.isRequired() && attrInfo.isMultiValued()
					&& attrInfo.getType().equals(String.class)) {
				continue;
			}
			if (attrInfo.is("favorite_number") && attrInfo.getNativeName().equals("favorite_number") && attrInfo.isCreateable()
					&& attrInfo.isUpdateable() && attrInfo.isReadable() && !attrInfo.isRequired() && !attrInfo.isMultiValued()
					&& attrInfo.getType().equals(Integer.class)) {
				continue;
			}
			if (attrInfo.is("full_name") && attrInfo.getNativeName().equals("full_name") && attrInfo.isCreateable()
					&& attrInfo.isUpdateable() && attrInfo.isReadable() && !attrInfo.isRequired() && !attrInfo.isMultiValued()
					&& attrInfo.getType().equals(String.class)) {
				continue;
			}
			throw new IllegalArgumentException("Schema is inconsistent");
		}
	}

	@Test(priority = 2)
	public void synchronizationTest(){
		KafkaConnector kafkaConnector = new KafkaConnector();
		disableSslVerification();
		KafkaConfiguration conf = getConsumerConfiguration();
		kafkaConnector.init(conf);
		
		OperationOptions options = new OperationOptions(new HashMap<String,Object>());
		
		ObjectClass objectClass = new ObjectClass(conf.getNameOfSchema());
		SyncResultsHandler handler = new SyncResultsHandler() {
			
			@Override
			public boolean handle(SyncDelta arg0) {
				LOGGER.info("Handler : {0}", arg0.getObject());
				return true;
			}
		};
		
		SyncToken token = kafkaConnector.getLatestSyncToken(objectClass);
		LOGGER.info("Token : {0}", token);
//		kafkaConnector.sync(objectClass, new SyncToken("P0-12;P2-10(1234567890)"), handler, options);
		kafkaConnector.sync(objectClass, new SyncToken("P0-0"), handler, options);

		kafkaConnector.sync(objectClass, token, handler, options);
		
		conf.release();
		kafkaConnector.dispose();
	}

	@Test(priority = 3)
	public void createTest(){
		KafkaConnector kafkaConnector = new KafkaConnector();
		disableSslVerification();
		KafkaConfiguration conf = getProducerConfiguration();

		kafkaConnector.init(conf);

		OperationOptions options = new OperationOptions(new HashMap<String,Object>());

		Set<Attribute> attributesAccount = new HashSet<Attribute>();
		attributesAccount.add(AttributeBuilder.build("favorite_number",1));
		attributesAccount.add(AttributeBuilder.build("full_name","Full Name Kafka test"));
		attributesAccount.add(new Uid("test_username"));
		attributesAccount.add(AttributeBuilder.build("test_array","test1", "test2"));
		attributesAccount.add(AttributeBuilder.build("test_record.name","nameTest"));
		attributesAccount.add(AttributeBuilder.build(OperationalAttributes.PASSWORD_NAME,new GuardedString("password".toCharArray())));

		ObjectClass objectClassAccount = new ObjectClass("Test schema");

		try {
			kafkaConnector.create(objectClassAccount, attributesAccount, options);
		} finally {
			conf.release();
			kafkaConnector.dispose();
		}

	}

	@Test(priority = 4)
	public void createAndReadTest(){
		KafkaConnector kafkaConnector = new KafkaConnector();
		disableSslVerification();
		KafkaConfiguration conf = getConsumerAndProducerConfiguration();
		kafkaConnector.init(conf);

		OperationOptions options = new OperationOptions(new HashMap<String,Object>());

		ConnectorObjectBuilder builder = new ConnectorObjectBuilder();
		ObjectClass objectClass = new ObjectClass("test_schema");
		builder.setObjectClass(objectClass);
		builder.setUid(new Uid("han_solo"));

		Set<Attribute> attributesAccount = new HashSet<Attribute>();
		attributesAccount.add(AttributeBuilder.build("favorite_number",1));
		attributesAccount.add(AttributeBuilder.build("full_name","Han Solo"));
		attributesAccount.add(new Name("han_solo"));
		attributesAccount.add(AttributeBuilder.build("test_array","Luke", "Leia"));
		attributesAccount.add(AttributeBuilder.build("test_record.name","han"));
		attributesAccount.add(AttributeBuilder.build(OperationalAttributes.PASSWORD_NAME,new GuardedString("chewbacca".toCharArray())));
		builder.addAttributes(attributesAccount);
		attributesAccount.add(new Uid("han_solo"));
		ConnectorObject connectorObject = builder.build();

		ArrayList<ConnectorObject> resultsAccount = new ArrayList<>();
		SyncResultsHandler handler = new SyncResultsHandler() {
			@Override
			public boolean handle(SyncDelta arg0) {
				LOGGER.info("Handler : {0}", arg0.getObject());
				resultsAccount.add(arg0.getObject());
				return true;
			}
		};

		try {
			kafkaConnector.create(objectClass, attributesAccount, options);
			kafkaConnector.sync(objectClass, new SyncToken("P0-0"), handler, options);
		} finally {
			conf.release();
			kafkaConnector.dispose();
		}

		Assert.assertEquals(resultsAccount.get(resultsAccount.size()-1), connectorObject);

	}

	@Test(priority = 5)
	public void deleteTest(){
		KafkaConnector kafkaConnector = new KafkaConnector();
		disableSslVerification();
		KafkaConfiguration conf = getProducerConfiguration();

		kafkaConnector.init(conf);

		OperationOptions options = new OperationOptions(new HashMap<String,Object>());

		Uid uid = new Uid("test_username");

		ObjectClass objectClassAccount = new ObjectClass("Test schema");

		try {
			kafkaConnector.delete(objectClassAccount, uid, options);
		} finally {
			conf.release();
			kafkaConnector.dispose();
		}
	}
	
}
