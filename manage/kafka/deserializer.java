package com.ibm.build2manage.messaging.kafka;

import io.cloudevents.CloudEvent;
import io.cloudevents.SpecVersion;
import io.cloudevents.core.format.EventDeserializationException;
import io.cloudevents.kafka.impl.KafkaHeaders;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Deserializer;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Implementation of {@link Deserializer} parsing a {@link CloudEvent} and returning the payload.
 *
 * @param <T> Type to be deserialized into.
 */
public class CloudEventDeserializer<T> implements Deserializer<T> {

    private static final Headers DEFAULT = new RecordHeaders(Arrays.asList(
            new RecordHeader(KafkaHeaders.CONTENT_TYPE, "application/cloudevents+json".getBytes()),
            new RecordHeader(KafkaHeaders.SPEC_VERSION, SpecVersion.V1.toString().getBytes())
    ));
    private final Deserializer<CloudEvent> delegate = new io.cloudevents.kafka.CloudEventDeserializer();

    private final KafkaAvroDeserializer avroDeserializer = new KafkaAvroDeserializer();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        delegate.configure(configs, isKey);
        avroDeserializer.configure(configs, isKey);
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        return deserialize(topic, DEFAULT, data);
    }

    @Override
    @SuppressWarnings("unchecked")
    public T deserialize(String topic, Headers headers, byte[] data) {


        CloudEvent event = delegate.deserialize(topic, headers, data);
        if (event.getDataContentType() == null || event.getData() == null) {
            return null;
        }
        try {
            byte[] avroData = event.getData().toBytes();
            GenericRecord record = (GenericRecord) avroDeserializer.deserialize(topic, avroData);
            return createTargetObject(event.getType(), record);
        } catch (Exception e) {
            throw new EventDeserializationException(e);
        }
    }

    private T createTargetObject(String className, GenericRecord record) throws ClassNotFoundException, InstantiationException, IllegalAccessException, NoSuchFieldException {

        Class<T> targetClass = (Class<T>) Class.forName(className);
        T targetObject = targetClass.newInstance();
        for (Schema.Field field : record.getSchema().getFields()) {
            String fieldName = field.name();
            Object value = extractValue(record, fieldName);
            boolean isMapHandled = false;
            if (value instanceof HashMap && targetClass.getDeclaredField(fieldName).getType().equals(Map.class)) {
                value = handleMapAttributes((Map<?, ?>) value, fieldName, targetObject, targetClass);
                isMapHandled = true;
            } else if (value instanceof List<?> || value instanceof GenericRecord) {
                isMapHandled = true;
            }
            if (!isMapHandled) {
                invokeSetterMethod(targetClass, targetObject, fieldName, value);
            }
        }
        return targetObject;
    }


    private Object extractValue(GenericRecord record, String fieldName) {
        Object value = record.get(fieldName);
        if (value instanceof Utf8) {
            return value.toString();
        } else if (value instanceof GenericData.Array || value instanceof List<?>) {
            return processListValues((List<?>) value);
        } else if (value instanceof GenericRecord) {
            return deserializeComplexObject((GenericRecord) value);
        } else if (value instanceof ByteBuffer) {
            ByteBuffer buffer = (ByteBuffer) value;
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            value = bytes;
            return value;
        }
        return value;
    }

    private List<Object> processGenericDataArray(GenericData.Array<?> array) {
        List<Object> processedList = new ArrayList<>();
        for (Object item : array) {
            if (item instanceof GenericRecord) {
                processedList.add(deserializeComplexObject((GenericRecord) item));
            } else {
                processedList.add(item);
            }
        }
        return processedList;
    }

    private void invokeSetterMethod(Class<T> targetClass, T targetObject, String fieldName, Object value) {
        if (value == null) return;
        String setterName = "set" + Character.toUpperCase(fieldName.charAt(0)) + fieldName.substring(1);
        try {
            Method setter = targetClass.getMethod(setterName, value.getClass());
            setter.invoke(targetObject, value);
        } catch (NoSuchMethodException e) {
            System.err.println("No setter found for property: " + fieldName + "with method name: " + setterName);
            //Handle exception or log it.
        } catch (IllegalAccessException e) {
            System.err.println("Unable to access setter for property: " + fieldName + ". Ensure the method is public.");
        } catch (InvocationTargetException e) {
            System.err.println("Setter for property: " + fieldName + "threw an exception.");
            e.getTargetException().printStackTrace();
        }
    }

    private Object deserializeComplexObject(GenericRecord genericRecordObject) {

        String className = (genericRecordObject).getSchema().getFullName();
        try {
            Class<?> targetClass = Class.forName(className);
            Object targetObject = targetClass.newInstance();
            for (Schema.Field field : ((GenericRecord) genericRecordObject).getSchema().getFields()) {
                String fieldName = field.name();
                Object value = extractValue(genericRecordObject, fieldName);
                if (value instanceof Utf8) {
                    value = value.toString();
                } else if (value instanceof Map<?, ?>) {
                    value = handleMapAttributes((Map<?, ?>) value, fieldName, targetObject, targetClass);
                }
                String setterName = "set" + Character.toUpperCase(fieldName.charAt(0)) + fieldName.substring(1);
                if (value != null) {
                    try {
                        Method setter = null;
                        if (value instanceof List) {
                            setter = findListSetter(targetClass, setterName);
                        } else {
                            setter = targetClass.getMethod(setterName, value.getClass());
                        }
                        setter.invoke(targetObject, value);
                    } catch (NoSuchMethodException e) {
                        System.err.println("No setter found for property: " + fieldName + "with method name: " + setterName);
                    } catch (IllegalAccessException e) {
                        System.err.println("Unable to access setter for property: " + fieldName + ". Ensure the method is public.");
                    } catch (InvocationTargetException e) {
                        System.err.println("Setter for property: " + fieldName + "threw an exception.");
                        e.getTargetException().printStackTrace();
                    }
                }
            }
            return targetObject;

        } catch (Exception e) {
            return null;
        }
    }

    private Method findListSetter(Class<?> targetClass, String setterName) throws NoSuchMethodException{
        for (Method method : targetClass.getMethods()) {
            if(method.getName().equals(setterName) && List.class.isAssignableFrom(method.getParameterTypes()[0])) {
                return method;
            }

        }
        throw new NoSuchMethodException("setter method not found for list: " +setterName);
    }

    private Object handleMapAttributes(Map<?, ?> originalMap, String fieldName, Object targetObject, Class<?> targetClass) {
        Map<String, Object> processedMap = new HashMap<>();
        for (Map.Entry<?, ?> entry : originalMap.entrySet()) {
            Object mapValue = entry.getValue();
            processedMap.put(entry.getKey().toString(), mapValue);
        }
        String setterName = "set" + Character.toUpperCase(fieldName.charAt(0)) + fieldName.substring(1);
        try {
            Method setter = targetClass.getMethod(setterName, Map.class);
            setter.invoke(targetObject, processedMap);
        } catch (NoSuchMethodException e) {
            System.err.println("No setter found for property: " + fieldName + "with method name: " + setterName);
        } catch (IllegalAccessException e) {
            System.err.println("Unable to access setter for property: " + fieldName + ". Ensure the method is public.");
        } catch (InvocationTargetException e) {
            System.err.println("Setter for property: " + fieldName + "threw an exception.");
            e.getTargetException().printStackTrace();
        }
        return processedMap;
    }

    private List<Object> processListValues(List<?> originalList) {
        List<Object> deserializedList = new ArrayList<>();
        for (Object listItem : originalList) {
            if (listItem instanceof GenericRecord) {
                deserializedList.add(deserializeComplexObject((GenericRecord) listItem));
            } else {
                deserializedList.add(listItem);
            }
        }
        return deserializedList;
    }
}