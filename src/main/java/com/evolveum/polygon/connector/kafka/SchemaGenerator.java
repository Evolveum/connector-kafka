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

import java.io.IOException;
import java.util.List;

import com.google.common.reflect.TypeToken;
import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaVersion;
import com.hortonworks.registries.schemaregistry.avro.AvroSchemaProvider;
import com.hortonworks.registries.schemaregistry.errors.IncompatibleSchemaException;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.identityconnectors.common.StringUtil;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.objects.*;

import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;

/**
 * @author skublik
 */
public class SchemaGenerator{

	private static final Log LOGGER = Log.getLog(KafkaConnector.class);

	private static final String SCHEMA_GROUP = "Kafka";

	private KafkaConfiguration configuration;
	private SchemaRegistryClient clientSchemaRegistry;

	public SchemaGenerator(KafkaConfiguration configuration, SchemaRegistryClient clientSchemaRegistry) {
		this.configuration = configuration;
		this.clientSchemaRegistry = clientSchemaRegistry;
	}

	public org.identityconnectors.framework.common.objects.Schema generateSchema() {
		SchemaBuilder schemaBuilder = new SchemaBuilder(KafkaConnector.class);
		Schema schema = null;

		if (configuration.isProducer()) {
			try {
				schema = KafkaConnectorUtils.getSchemaForProducerFromFile(configuration);
				SchemaMetadata.Builder schemaMetadataBuilder = new SchemaMetadata.Builder(configuration.getNameOfSchema());
				schemaMetadataBuilder.type(AvroSchemaProvider.TYPE).schemaGroup(SCHEMA_GROUP)
						.description("Schema registered by SchemaGenerator for topic: [" + configuration.getProducerNameOfTopic() + "]");
				SchemaIdVersion schemaIdVersion = clientSchemaRegistry.addSchemaVersion(schemaMetadataBuilder.build(),
						new SchemaVersion(schema.toString(), "Schema registered by SchemaGenerator of KafkaConnector"));
				if (configuration.isConsumer()) {
					configuration.setConsumerVersionOfSchema(schemaIdVersion.getVersion());
				}
			} catch (IOException e) {
				String message = "Couldn't get avro schema from file " + configuration.getProducerPathToFileContainingSchema();
				LOGGER.error(message, e);
				return null;
			} catch (InvalidSchemaException e) {
				String message = "Couldn't parse avro schema from file " + configuration.getProducerPathToFileContainingSchema();
				LOGGER.error(message, e);
				return null;
			} catch (SchemaNotFoundException e) {
				String message = "Couldn't found schema with name " + configuration.getNameOfSchema();
				LOGGER.error(message, e);
				return null;
			} catch (IncompatibleSchemaException e) {
				String message = "Schema from file " + configuration.getProducerPathToFileContainingSchema() + " is incompatible with previous version";
				LOGGER.error(message, e);
				return null;
			}

		} else if (configuration.isConsumer()) {
			String schemaName = configuration.getNameOfSchema();
			Integer schemaVersion = configuration.getConsumerVersionOfSchema();
			try {
				SchemaVersionInfo versionInfo = KafkaConnectorUtils.getSchemaVersionInfo(configuration, clientSchemaRegistry);
				Schema.Parser parser = new Schema.Parser();
				schema = parser.parse(versionInfo.getSchemaText());
			} catch (SchemaNotFoundException e) {
				StringBuilder sb = new StringBuilder();
				sb.append("Schema with name ").append(schemaName);
				if (schemaVersion != null) {
					sb.append(" and version ").append(schemaVersion).append(" not found");
				}
				LOGGER.error(sb.toString(), e);
				return null;
			}
		}

		LOGGER.info("Parsing schema with name " + schema.getName());

		ObjectClassInfoBuilder objClassBuilder = new ObjectClassInfoBuilder();
		String objectClassType = StringUtils.isBlank(schema.getName()) ? schema.getFullName() : schema.getName();
		objClassBuilder.setType(objectClassType);
		LOGGER.ok("Create ObjectClass with type " + objectClassType);
		processRecord(schema, objClassBuilder, "");
		LOGGER.info("----------------------------------------------------------------------------");
		schemaBuilder.defineObjectClass(objClassBuilder.build());

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
			throw new ConnectorException(sb.toString());
		}
		for(Field field : fields) {
			if(field == null) {
				StringBuilder sb = new StringBuilder();
				sb.append("Field is null");
				LOGGER.error(sb.toString());
				throw new ConnectorException(sb.toString());
			}
			Schema fieldSchema = field.schema();
			if(fieldSchema == null) {
				StringBuilder sb = new StringBuilder();
				sb.append("Schema of field with name ").append(field.name()).append(" is null");
				LOGGER.error(sb.toString());
				throw new ConnectorException(sb.toString());
			}
			LOGGER.ok("----Parsing field with name " + field.name() + " and type " + fieldSchema.getType().getName());
			if(fieldSchema.getType().equals(Schema.Type.UNION)) {
				Type schemaType = getTypeForUnion(fieldSchema);
				if (isArray(schemaType)) {
					Schema arraySchema = fieldSchema.getTypes().get(0);
					if (isNull(arraySchema.getType())) {
						arraySchema = fieldSchema.getTypes().get(1);
					}
					processArray(field, arraySchema, nameOfParent, objClassBuilder);
				} else {
					addAttributeToObjectClass(nameOfParent + field.name(), schemaType, false, false, objClassBuilder);
				}
			} else if(fieldSchema.getType().equals(Schema.Type.RECORD)) {
				processRecord(fieldSchema, objClassBuilder, field.name());
			}
			else if(isArray(fieldSchema.getType())) {
				processArray(field, fieldSchema, nameOfParent, objClassBuilder);
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
				throw new ConnectorException(sb.toString());
			}
		};
		
	}

	private void processArray(Field field, Schema fieldSchema, String nameOfParent, ObjectClassInfoBuilder objClassBuilder) {
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
			throw new ConnectorException(message);
		}
		boolean isRequired = true;
		if(field.defaultVal() != null) {
			isRequired = false;
		}
		addAttributeToObjectClass(nameOfParent + field.name(), valueType, isRequired, true, objClassBuilder);
	}

	public static Type getTypeForUnion(Schema fieldSchema) {
		List<Schema> types = fieldSchema.getTypes();
		if(types == null || types.size() != 2 ||
				!((isNull(types.get(0).getType()) ||
						isNull(types.get(1).getType()))
						&& ((isPrimitive(types.get(0).getType()) || isArray(types.get(0).getType())) ||
						(isPrimitive(types.get(1).getType()) || isArray(types.get(0).getType()))))) {
			StringBuilder sb = new StringBuilder();
			sb.append("Connector support 'union' only with two types, where one of its have to be 'null' and second type have to be primitive type or 'array'");
			LOGGER.error(sb.toString());
			throw new IllegalStateException(sb.toString());
		}
		Type schemaType = null;
		for(Schema type : types) {
			if(!type.getType().equals(Schema.Type.NULL)) {
				schemaType = type.getType();
			}
		}
		return schemaType;
	}

	private void addAttributeToObjectClass(String name, Type schemaType, boolean isRequired,
			boolean isMultivalue, ObjectClassInfoBuilder objClassBuilder) {

		boolean isUpdateble = configuration.isProducer();
		boolean addAttribute = true;
		if (name.equals(configuration.getUniqueAttribute())) {
			if (!Type.STRING.equals(schemaType)) {
				throw new ConnectorException("Type of attribute for name is " + schemaType + " but expected is STRING.");
			}
			LOGGER.ok("--------Adding unique attribute with name " + name);
			// unique column
			AttributeInfoBuilder attrBuilder = new AttributeInfoBuilder(Uid.NAME);
			attrBuilder.setType(String.class);
			attrBuilder.setNativeName(name);
			attrBuilder.setRequired(true).setCreateable(isUpdateble).setUpdateable(isUpdateble);
			objClassBuilder.addAttributeInfo(attrBuilder.build());

			if(StringUtil.isBlank(configuration.getNameAttribute())) {
				LOGGER.ok("--------Adding name attribute with name " + name);
				attrBuilder = new AttributeInfoBuilder(Name.NAME);
				attrBuilder.setType(String.class);
				attrBuilder.setNativeName(name);
				attrBuilder.setRequired(true).setCreateable(isUpdateble).setUpdateable(isUpdateble);
				objClassBuilder.addAttributeInfo(attrBuilder.build());
				addAttribute = false;
			}
			
		} 
		if (name.equals(configuration.getNameAttribute())) {
			LOGGER.ok("--------Adding name attribute with name " + name);
			AttributeInfoBuilder attrBuilder = new AttributeInfoBuilder(Name.NAME);
			if (!Type.STRING.equals(schemaType)) {
				throw new ConnectorException("Type of attribute for name is " + schemaType + " but expected is STRING.");
			}
			attrBuilder.setType(String.class);
			attrBuilder.setNativeName(name);

			if (isRequired || (StringUtil.isNotBlank(configuration.getNameAttribute()))
								&& configuration.getUniqueAttribute().equals(configuration.getNameAttribute())) {
				attrBuilder.setRequired(true);
			}
			attrBuilder.setCreateable(isUpdateble).setUpdateable(isUpdateble);

			objClassBuilder.addAttributeInfo(attrBuilder.build());
			addAttribute = false;
		}
		if (name.equals(configuration.getPasswordAttribute())) {
			LOGGER.ok("--------Adding password attribute with name " + name);
			if (!Type.STRING.equals(schemaType)) {
				throw new ConnectorException("Type of attribute for name is " + schemaType + " but expected is STRING.");
			}
			AttributeInfoBuilder attrBuilder = new AttributeInfoBuilder(OperationalAttributes.PASSWORD_NAME);
			attrBuilder.setNativeName(name).setRequired(isRequired).setType(GuardedString.class)
					.setCreateable(isUpdateble).setUpdateable(isUpdateble).setReadable(true);

			objClassBuilder.addAttributeInfo(attrBuilder.build());
			addAttribute = false;
		}
		if(addAttribute){
			LOGGER.ok("--------Adding attribute with name " + name);
			AttributeInfoBuilder attrBuilder = new AttributeInfoBuilder(name);
			attrBuilder.setNativeName(name).setRequired(isRequired).setType(getType(schemaType))
			.setCreateable(isUpdateble).setUpdateable(isUpdateble).setReadable(true);
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

	public static boolean isPrimitive(Type type) {
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

	private static boolean isArray(Type type) {
		if(type.equals(Type.ARRAY)) {
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
