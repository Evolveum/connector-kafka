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

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

import org.apache.avro.generic.GenericData.Record;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.Validate;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.identityconnectors.common.StringUtil;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.common.FrameworkUtil;
import org.identityconnectors.framework.common.exceptions.ConnectorIOException;
import org.identityconnectors.framework.common.objects.*;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * @author skublik
 */
public class Synchronizer {

	private static final Log LOGGER = Log.getLog(KafkaConnector.class);
	
	private KafkaConfiguration configuration;
	private KafkaConsumer<Object, Object> consumer;
	
	public Synchronizer(KafkaConfiguration configuration, KafkaConsumer<Object, Object> consumer){
		this.configuration = configuration;
		this.consumer = consumer;
	}
	
	public void synchronize(ObjectClass objectClass, SyncToken token, SyncResultsHandler handler, OperationOptions options){
		List<TopicPartition> partitions = KafkaConnectorUtils.getPatritions(configuration);
		LOGGER.ok("Used partitions: {0}", partitions);
		consumer.assign(partitions);
		if(token == null || token.getValue() == null) {
			throw new IllegalArgumentException("Value of token is null");
		}
		String lastToken = (String) getLatestSyncToken().getValue();
		if (token.getValue().equals(lastToken)){
			LOGGER.ok("This is last token: {0}", lastToken);
			return;
		}
		String stringToken = (String) token.getValue();
		if(stringToken.contains("(")) {
			String stringLong = stringToken.substring(stringToken.indexOf("(") + 1, stringToken.lastIndexOf(")"));
			long longToken = Long.parseLong(stringLong);
			LOGGER.ok("Used timestamp token: {0}", longToken);
			Map<TopicPartition, Long> timestampsToSearch = new HashMap<TopicPartition, Long>();
			for(TopicPartition partition : partitions) {
				timestampsToSearch.put(partition, longToken);
			}
			Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes = consumer.offsetsForTimes(timestampsToSearch);
			for(TopicPartition partition : offsetsForTimes.keySet()) {
				LOGGER.ok("Used offset {0} for partitions: {0}", offsetsForTimes.get(partition).offset(), partitions);
				consumer.seek(partition, offsetsForTimes.get(partition).offset());
			}
		} else {
			Map<Integer, Long> offsets = new HashMap<Integer, Long>();
			for(String stringPartition : stringToken.split(";")) {
				String stringNumberOfPartition = stringPartition.substring(1, stringPartition.lastIndexOf("-"));
				int numberOfPartition = Integer.parseInt(stringNumberOfPartition);
				long offset = Long.parseLong(stringPartition.substring(stringPartition.lastIndexOf("-")+1));
				offsets.put(numberOfPartition, offset);
			}
			LOGGER.ok("Received offsets from token: {0}", offsets);
			for(TopicPartition partition : partitions) {
				if(offsets.containsKey(partition.partition())) {
					LOGGER.ok("Seek offset " + offsets.get(partition.partition()));
					consumer.seek(partition, offsets.get(partition.partition()));
				}
			}
		}
		
		Map<Integer, Long> newOffsets = new HashMap<Integer, Long>();
		Integer duration = configuration.getConsumerDurationIfFail() == null ? 2 : configuration.getConsumerDurationIfFail();
		ConsumerRecords<Object, Object> records = consumer.poll(TimeUnit.MINUTES.toMillis(duration));
		StreamSupport.stream(records.spliterator(), false).forEach(record -> {
			processingRecords(record, handler, newOffsets);
		});
		consumer.commitSync();
	}
	
	private void processingRecords(ConsumerRecord<Object, Object> record, SyncResultsHandler handler, Map<Integer, Long> newOffsets) {
		Validate.notNull(record, "Received record is null");
		Validate.notNull(record.key(), "Key of received record is null");
		
		LOGGER.ok("key of record is {0}", record.key());
		LOGGER.ok("value of record is {0}", record.value());
	    if (record.value() == null) {

//	    	JSONObject key = new JSONObject(record.key().toString());
//			Validate.notNull(key, "Json generated from key of received record is null");
//			if(key.get(configuration.getUniqueAttribute()) == null) {
//				throw new ConnectorIOException("Json generated from key of received record not "
//						+ "contains unique attribute " + configuration.getUniqueAttribute());
//			}

	    	String key = (String) record.key();
	    	SyncDeltaBuilder synDeltaB = new SyncDeltaBuilder();
	    	ConnectorObjectBuilder builder = new ConnectorObjectBuilder();
  			builder.setObjectClass(new ObjectClass(configuration.getNameOfSchema()));
  			builder.setUid(String.valueOf(key));
	      	ConnectorObject connectorObject = builder.build();
			synDeltaB.setToken(new SyncToken(record.offset()));
			synDeltaB.setObject(connectorObject);
			synDeltaB.setDeltaType(SyncDeltaType.DELETE);
			handler.handle(synDeltaB.build());
	    } else if(record.value() instanceof Record) {
	      	JSONObject object = new JSONObject(record.value().toString());
	      	if(object != null && !object.isEmpty()) {
	      		ConnectorObjectBuilder builder = new ConnectorObjectBuilder();
	    		builder.setObjectClass(new ObjectClass(configuration.getNameOfSchema()));
	      		convertJSONObjectToConnectorObject(object, builder, "");
	      		ConnectorObject connectorObject = builder.build();
	      		LOGGER.ok("processingRecords, connectorObject: {0}", connectorObject.toString());
	      		SyncDeltaBuilder synDeltaB = new SyncDeltaBuilder();
	      		if(record.offset() == Long.MAX_VALUE) {
	      			newOffsets.put(record.partition(), 0L);
	      		} else {
	      			newOffsets.put(record.partition(), record.offset() + 1);
	      		}
	      		String token = KafkaConnectorUtils.parseToken(newOffsets);
	      		synDeltaB.setToken(new SyncToken(token));//.offset() + 1));
	      		
	      		LOGGER.ok("Token for this record {0}", token);
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
				LOGGER.ok("convertJSONObjectToConnectorObject, read attribute with name: {0}, value: null", parentName + name);
				builder.addAttribute(AttributeBuilder.build(parentName + name));
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
					LOGGER.ok("convertJSONObjectToConnectorObject, read attribute with name: {0}, value: {1}", Uid.NAME, object.get(name));
					builder.setUid(String.valueOf(object.get(name)));
					if(StringUtil.isBlank(configuration.getNameAttribute())) {
						LOGGER.ok("convertJSONObjectToConnectorObject, read attribute with name: {0}, value: {1}", Name.NAME, object.get(name));
						builder.setName(String.valueOf(object.get(name)));
						continue;
					}
				} 
				if (name.equals(configuration.getNameAttribute())) {
					LOGGER.ok("convertJSONObjectToConnectorObject, read attribute with name: {0}, value: {1}", Name.NAME, object.get(name));
					builder.setName(String.valueOf(object.get(name)));
					continue;
				}
				if (name.equals(configuration.getPasswordAttribute())) {
					LOGGER.ok("convertJSONObjectToConnectorObject, read attribute with name: {0}, value: [hidden]", OperationalAttributes.PASSWORD_NAME);
					builder.addAttribute(OperationalAttributes.PASSWORD_NAME, new GuardedString(((String)object.get(name)).toCharArray()));
					continue;
				}
				LOGGER.ok("convertJSONObjectToConnectorObject, read attribute with name: {0}, value: {1}", parentName + name, object.get(name));
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
		List<TopicPartition> partitions = KafkaConnectorUtils.getPatritions(configuration);
		consumer.assign(partitions);
		Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
		Map<Integer, Long> offsets = new HashMap<Integer, Long>();
		
		for (TopicPartition partition : endOffsets.keySet()) {
			offsets.put(partition.partition(), endOffsets.get(partition));
		}
		String token = KafkaConnectorUtils.parseToken(offsets);
		return new SyncToken(token);
	}
	
}
