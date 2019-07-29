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
import java.util.Arrays;
import java.util.List;
import java.util.stream.StreamSupport;

import org.apache.avro.generic.GenericData.Record;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.Validate;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.identityconnectors.common.StringUtil;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.FrameworkUtil;
import org.identityconnectors.framework.common.exceptions.ConnectorIOException;
import org.identityconnectors.framework.common.objects.AttributeBuilder;
import org.identityconnectors.framework.common.objects.ConnectorObject;
import org.identityconnectors.framework.common.objects.ConnectorObjectBuilder;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.OperationOptions;
import org.identityconnectors.framework.common.objects.SyncDeltaBuilder;
import org.identityconnectors.framework.common.objects.SyncDeltaType;
import org.identityconnectors.framework.common.objects.SyncResultsHandler;
import org.identityconnectors.framework.common.objects.SyncToken;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * @author skublik
 */
public class Synchronizer {

	private static final Log LOGGER = Log.getLog(Synchronizer.class);
	
	private KafkaConfiguration configuration;
	private KafkaConsumer<Object, Object> consumer;
	
	public Synchronizer(KafkaConfiguration configuration, KafkaConsumer<Object, Object> consumer){
		this.configuration = configuration;
		this.consumer = consumer;
	}
	
	public void synchronize(ObjectClass objectClass, SyncToken token, SyncResultsHandler handler, OperationOptions options){
		TopicPartition partition = new TopicPartition(configuration.getConsumerNameOfTopic(), configuration.getPartitionOfTopic());
		List partitions = Arrays.asList(partition);
		consumer.assign(partitions);
		if(token != null || token.getValue() != null) {
			consumer.seek(partition, (long)token.getValue());
		}
		
		ConsumerRecords<Object, Object> records = consumer.poll(1_000L);
		StreamSupport.stream(records.spliterator(), false).forEach(record -> {
			processingRecords(record, handler);
		});
		consumer.commitSync();
	}
	
	private void processingRecords(ConsumerRecord<Object, Object> record, SyncResultsHandler handler) {
		Validate.notNull(record, "Received record is null");
		Validate.notNull(record.key(), "Key of received record is null");
		
		LOGGER.info("key of record is {0}", record.key());
		LOGGER.info("value of record is {0}", record.value());
//	    if (record.value() == null) {
//	    	
////	    	JSONObject key = new JSONObject(record.key().toString());
////			Validate.notNull(key, "Json generated from key of received record is null");
////			if(key.get(configuration.getUniqueAttribute()) == null) {
////				throw new ConnectorIOException("Json generated from key of received record not "
////						+ "contains unique attribute " + configuration.getUniqueAttribute()); 
////			}
//	      
//	    	String key = (String) record.key();
//	    	SyncDeltaBuilder synDeltaB = new SyncDeltaBuilder();
//	    	ConnectorObjectBuilder builder = new ConnectorObjectBuilder();
//  			builder.setObjectClass(new ObjectClass(configuration.getNameOfSchema()));
////  			builder.setUid(String.valueOf(key.get(configuration.getUniqueAttribute())));
////  			builder.setName(String.valueOf(key.get(configuration.getUniqueAttribute())));
//  			builder.setUid(String.valueOf(key));
//			builder.setName(String.valueOf(key));
//	      	ConnectorObject connectorObject = builder.build();
//			synDeltaB.setToken(new SyncToken(record.offset()));
//			synDeltaB.setObject(connectorObject);
//			synDeltaB.setDeltaType(SyncDeltaType.DELETE);
//			handler.handle(synDeltaB.build());
//	    } else {
	      if(record.value() instanceof Record) {
	      	JSONObject object = new JSONObject(record.value().toString());
	      	if(object != null && !object.isEmpty()) {
	      		ConnectorObjectBuilder builder = new ConnectorObjectBuilder();
	    		builder.setObjectClass(new ObjectClass(configuration.getNameOfSchema()));
	      		convertJSONObjectToConnectorObject(object, builder, "");
	      		ConnectorObject connectorObject = builder.build();
	      		LOGGER.info("convertObjectToConnectorObject, object: {0}, \n\tconnectorObject: {1}", object, connectorObject.toString());
	      		SyncDeltaBuilder synDeltaB = new SyncDeltaBuilder();
	      		synDeltaB.setToken(new SyncToken(record.offset() + 1));
				synDeltaB.setObject(connectorObject);
				synDeltaB.setDeltaType(SyncDeltaType.CREATE_OR_UPDATE);
				handler.handle(synDeltaB.build());
//	      		handler.handle(connectorObject);
	      	} else {
	      		String errorMessage = "Json object generated from Record is null or empry";
		    	LOGGER.error(errorMessage);
		    	throw new ConnectorIOException(errorMessage);
	      	}
	      }
	      else {
	    	  String errorMessage = "Connector support only record as basic object";
	    	  LOGGER.error(errorMessage);
	    	  throw new ConnectorIOException(errorMessage);
	      }
//	    }
	}
	
	private void convertJSONObjectToConnectorObject(JSONObject object, ConnectorObjectBuilder builder, String parentName) {
		
		if(object.isEmpty()) {
			LOGGER.warn("JSONObject is empty");
		}
		
		if(StringUtil.isBlank(parentName)) {
			parentName = "";
		} else {
			parentName += ".";
		}
		
		for(String name : object.keySet()){
			if(object.get(name).equals(null)) {
				builder.addAttribute(AttributeBuilder.build(name));
			} else if(object.get(name) instanceof JSONObject) {
				convertJSONObjectToConnectorObject((JSONObject)object.get(name), builder, name);
			} else if(object.get(name) instanceof JSONArray) {
				JSONArray array = (JSONArray)object.get(name);
				List<Object> values = new ArrayList<>();
				for (int i = 0; i < array.length(); i++) {
					Object value = array.get(i);
					FrameworkUtil.checkAttributeType(value.getClass());
					values.add(value);
				}
				builder.addAttribute(parentName + name, values);
			} else {
				
				if (name.equals(configuration.getUniqueAttribute())) {
					builder.setUid(String.valueOf(object.get(name)));
					if(StringUtil.isBlank(configuration.getNameAttribute())) {
						builder.setName(String.valueOf(object.get(name)));
					}
				} 
				if (name.equals(configuration.getNameAttribute())) {
					builder.setName(String.valueOf(object.get(name)));
				}
				builder.addAttribute(parentName + name, object.get(name));
			}
		}
	}

	protected <T> T addAttr(ConnectorObjectBuilder builder, String attrName, T attrVal) {
		if (attrVal != null) {
			if(attrVal instanceof String){
				String unescapeAttrVal = StringEscapeUtils.unescapeXml((String)attrVal);
				builder.addAttribute(attrName, unescapeAttrVal);
			} else {
				builder.addAttribute(attrName, attrVal);
			}
		}
		return attrVal;
	}

	public SyncToken getLatestSyncToken() {
		TopicPartition assignment = new TopicPartition(configuration.getConsumerNameOfTopic(), configuration.getPartitionOfTopic());
		List assignments = Arrays.asList(assignment);
		consumer.assign(assignments);
		long end = (long)consumer.endOffsets(assignments).get(assignment);
//		if(end > 0) {
//			end--;
//		}
		SyncToken ret = new SyncToken(end);
		return new SyncToken(end);
	}
	
}
