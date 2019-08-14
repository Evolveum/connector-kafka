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

import java.util.HashMap;

import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.OperationOptions;
import org.identityconnectors.framework.common.objects.Schema;
import org.identityconnectors.framework.common.objects.SyncDelta;
import org.identityconnectors.framework.common.objects.SyncResultsHandler;
import org.identityconnectors.framework.common.objects.SyncToken;
import org.testng.annotations.Test;

/**
 * @author skublik
 */
public class BasicTests extends BasicFunctionsForTests {

	private static final Log LOGGER = Log.getLog(BasicTests.class);
	
//	@Test(priority = 1)
	public void schemaTest(){
		KafkaConnector kafkaConnector = new KafkaConnector();
		disableSslVerification();
		KafkaConfiguration conf = getConfiguration();
		kafkaConnector.init(conf);
		
		Schema schema = kafkaConnector.schema();
		LOGGER.info("Schema : {0}", schema.toString());
		conf.release();
		kafkaConnector.dispose();
	}
	
	@Test(priority = 2)
	public void synchronizationTest(){
		KafkaConnector kafkaConnector = new KafkaConnector();
		disableSslVerification();
		KafkaConfiguration conf = getConfiguration();
		kafkaConnector.init(conf);
		
		OperationOptions options = new OperationOptions(new HashMap<String,Object>());
		
		ObjectClass objectClass = new ObjectClass(conf.getNameOfSchema());
		SyncResultsHandler handler = new SyncResultsHandler() {
			
			@Override
			public boolean handle(SyncDelta arg0) {
				LOGGER.info("Handler : {0}", arg0.getObject().getName());
				return true;
			}
		};
		
		SyncToken token = kafkaConnector.getLatestSyncToken(objectClass);
		LOGGER.info("Token : {0}", token);
		kafkaConnector.sync(objectClass, new SyncToken("P0-12;P2-10(1234567890)"), handler, options);
		
//		kafkaConnector.sync(objectClass, token, handler, options);
		
		conf.release();
		kafkaConnector.dispose();
	}
	
}
