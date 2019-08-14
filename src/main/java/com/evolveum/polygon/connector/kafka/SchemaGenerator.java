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

import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.identityconnectors.common.StringUtil;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.objects.AttributeInfoBuilder;
import org.identityconnectors.framework.common.objects.Name;
import org.identityconnectors.framework.common.objects.ObjectClassInfoBuilder;
import org.identityconnectors.framework.common.objects.SchemaBuilder;
import org.identityconnectors.framework.common.objects.Uid;

import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;

/**
 * @author skublik
 */
public class SchemaGenerator{
	
	private static final Log LOGGER = Log.getLog(KafkaConnector.class);
	
	private KafkaConfiguration configuration;
	private SchemaRegistryClient clientSchemaRegistry;
	
	public SchemaGenerator(KafkaConfiguration configuration, SchemaRegistryClient clientSchemaRegistry) {
		this.configuration = configuration;
		this.clientSchemaRegistry = clientSchemaRegistry;
	}
	
	public org.identityconnectors.framework.common.objects.Schema generateSchema() {
		
		SchemaBuilder schemaBuilder = new SchemaBuilder(KafkaConnector.class);
		String schemaName = configuration.getNameOfSchema();
		Integer schemaVersion = configuration.getVersionOfSchema();
		try {
			SchemaVersionInfo versionInfo = KafkaConnectorUtils.getSchemaVersionInfo(configuration, clientSchemaRegistry);
			Schema.Parser parser = new Schema.Parser(); 
			Schema schema = parser.parse(versionInfo.getSchemaText());
			LOGGER.info("Parsing schema with name " + schemaName);
			
			ObjectClassInfoBuilder objClassBuilder = new ObjectClassInfoBuilder();
			String objectClassType = StringUtils.isBlank(schema.getName()) ? schemaName : schema.getName();
			objClassBuilder.setType(objectClassType);
			LOGGER.ok("Create ObjectClass with type " + objectClassType);
			processRecord(schema, objClassBuilder, "");
			LOGGER.info("----------------------------------------------------------------------------");
			schemaBuilder.defineObjectClass(objClassBuilder.build());
		} catch (SchemaNotFoundException e) {
			StringBuilder sb = new StringBuilder();
			sb.append("Schema with name ").append(schemaName)
			.append(" and type ").append(schemaVersion).append(" not found");
			LOGGER.error(sb.toString(), e);
		}
		return schemaBuilder.build();
	}

	

	private void processRecord(Schema schema,
			ObjectClassInfoBuilder objClassBuilder, String nameOfParent) {
		
		if(StringUtil.isBlank(nameOfParent)) {
			nameOfParent = "";
		} else {
			nameOfParent += ".";
		}
		
		List<Field> fields = schema.getFields();
		
		if(fields == null || fields.isEmpty()) {
			StringBuilder sb = new StringBuilder();
			sb.append("Fields of schema is null or empty").append(fields);
			LOGGER.error(sb.toString());
			throw new IllegalStateException(sb.toString());
		}
		for(Field field : fields) {
			if(field == null) {
				StringBuilder sb = new StringBuilder();
				sb.append("Field is null");
				LOGGER.error(sb.toString());
				throw new IllegalStateException(sb.toString());
			}
			Schema fieldSchema = field.schema();
			if(fieldSchema == null) {
				StringBuilder sb = new StringBuilder();
				sb.append("Schema of field with name ").append(field.name()).append(" is null");
				LOGGER.error(sb.toString());
				throw new IllegalStateException(sb.toString());
			}
			LOGGER.ok("----Parsing field with name " + field.name() + " and type " + fieldSchema.getType().getName());
			if(fieldSchema.getType().equals(Schema.Type.UNION)) {
				List<Schema> types = fieldSchema.getTypes();
				if(types == null || types.size() != 2 ||
						!((isNull(types.get(0).getType()) ||
					      isNull(types.get(1).getType()))
					 && (isPrimitive(types.get(0).getType()) ||
						isPrimitive(types.get(1).getType())))) {
					StringBuilder sb = new StringBuilder();
					sb.append("Connector support 'union' only with two primitive types, where one of its must be 'null'");
					LOGGER.error(sb.toString());
					throw new IllegalStateException(sb.toString());
				}
				Type schemaType = null;
				for(Schema type : types) {
					if(!type.getType().equals(Schema.Type.NULL)) {
						schemaType = type.getType();
					}
				}
				addAttributeToObjectClass(nameOfParent + field.name(), schemaType, false, false, objClassBuilder);
			} else if(fieldSchema.getType().equals(Schema.Type.RECORD)) {
				processRecord(fieldSchema, objClassBuilder, field.name());
			}
			else if(fieldSchema.getType().equals(Schema.Type.ARRAY)) {
				StringBuilder sb = new StringBuilder();
				sb.append("ElementType for '").append(fieldSchema.getName()).append("' is null");
				Validate.notNull(fieldSchema.getElementType().getType(), sb.toString());
				
				sb = new StringBuilder();
				sb.append("Type in ElementType for '").append(fieldSchema.getName()).append("' is null");
				Type valueType = fieldSchema.getElementType().getType();
				Validate.notNull(valueType, sb.toString());
				
				if(!isPrimitive(valueType)) {
					String message = "Connector support only array with primitive type";
					LOGGER.error(message);
					throw new IllegalStateException(message);
				}
				boolean isRequired = true;
				if(field.defaultVal() != null) {
					isRequired = false;
				}
				addAttributeToObjectClass(nameOfParent + field.name(), valueType, isRequired, true, objClassBuilder);
			}else if(isPrimitive(fieldSchema.getType())) {
				boolean isRequired = true;
				if(field.defaultVal() != null) {
					isRequired = false;
				}
				addAttributeToObjectClass(nameOfParent + field.name(), fieldSchema.getType(), isRequired, false, objClassBuilder);
			} else {
				StringBuilder sb = new StringBuilder();
				sb.append("Connector don't support 'enum','map' and 'fixed'");
				LOGGER.error(sb.toString());
				throw new IllegalStateException(sb.toString());
			}
		};
		
	}

	private void addAttributeToObjectClass(String name, Type schemaType, boolean isRequired,
			boolean isMultivalue, ObjectClassInfoBuilder objClassBuilder) {
		
		boolean addAttribute = true;
		if (name.equals(configuration.getUniqueAttribute())) {
			LOGGER.ok("--------Adding unique attribute with name " + name);
			// unique column
			AttributeInfoBuilder attrBuilder = new AttributeInfoBuilder(Uid.NAME);
			attrBuilder.setType(String.class);
			attrBuilder.setNativeName(name);
			attrBuilder.setRequired(true).setCreateable(false).setUpdateable(false);
			objClassBuilder.addAttributeInfo(attrBuilder.build());

			if(StringUtil.isBlank(configuration.getNameAttribute())) {
				attrBuilder = new AttributeInfoBuilder(Name.NAME);
				attrBuilder.setType(String.class);
				attrBuilder.setNativeName(name);
				attrBuilder.setRequired(true).setCreateable(false).setUpdateable(false);
				objClassBuilder.addAttributeInfo(attrBuilder.build());
				addAttribute = false;
			}
			
		} 
		if (name.equals(configuration.getNameAttribute())) {
			AttributeInfoBuilder attrBuilder = new AttributeInfoBuilder(Name.NAME);
			attrBuilder.setType(String.class);
			attrBuilder.setNativeName(name);

			if (isRequired || (StringUtil.isNotBlank(configuration.getNameAttribute()))
								&& configuration.getUniqueAttribute().equals(configuration.getNameAttribute())) {
				attrBuilder.setRequired(true);
			}
			attrBuilder.setCreateable(false).setUpdateable(false);

			objClassBuilder.addAttributeInfo(attrBuilder.build());
			addAttribute = false;
		}
		if(addAttribute){
			AttributeInfoBuilder attrBuilder = new AttributeInfoBuilder(name);
			attrBuilder.setNativeName(name).setRequired(isRequired).setType(getType(schemaType))
			.setCreateable(false).setUpdateable(false).setReadable(true);
			if(!name.equals(configuration.getUniqueAttribute())) {
				attrBuilder.setMultiValued(isMultivalue);
			}
			objClassBuilder.addAttributeInfo(attrBuilder.build());
		}
	}

	private static Class<?> getType(Type schemaType) {
		switch (schemaType) {
		case BOOLEAN:
			return Boolean.class;
		case DOUBLE:
			return Double.class;
		case BYTES:
			return Byte.class;
		case FLOAT:
			return Float.class;
		case INT:
			return Integer.class;
		case LONG:
			return Long.class;
		case STRING:
			return String.class;
		default:
			return null;
		}
	}

	private static boolean isPrimitive(Type type) {
		if(type.equals(Schema.Type.BOOLEAN) ||
				type.equals(Schema.Type.DOUBLE) ||
				type.equals(Schema.Type.BYTES) ||
				type.equals(Schema.Type.FLOAT) ||
				type.equals(Schema.Type.INT) ||
				type.equals(Schema.Type.LONG) ||
				type.equals(Schema.Type.STRING)) {
			return true;
		}
		return false;
	}
	
	private static boolean isNull(Type type) {
		if(type.equals(Schema.Type.NULL)) {
			return true;
		}
		return false;
	}
}
