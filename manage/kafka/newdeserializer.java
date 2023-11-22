// ... [other parts of the class remain the same]

private Object extractValue(GenericRecord record, String fieldName) {
    Object value = record.get(fieldName);
    if (value instanceof Utf8) {
        return value.toString();
    } else if (value instanceof GenericData.Array) {
        return processGenericDataArray((GenericData.Array<?>) value);
    } else if (value instanceof GenericRecord) {
        return deserializeComplexObject((GenericRecord) value);
    } else if (value instanceof ByteBuffer) {
        ByteBuffer buffer = (ByteBuffer) value;
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }
    return value;
}

private List<Object> processGenericDataArray(GenericData.Array<?> array) {
    List<Object> processedList = new ArrayList<>();
    for (Object item : array) {
        if (item instanceof GenericRecord) {
            processedList.add(deserializeComplexObject((GenericRecord) item));
        } else if (item instanceof Map<?, ?>) {
            processedList.add(processMapValues((Map<?, ?>) item));
        } else if (item instanceof List<?>) {
            processedList.add(processListValues((List<?>) item));
        } else {
            processedList.add(item);
        }
    }
    return processedList;
}

private Map<String, Object> processMapValues(Map<?, ?> originalMap) {
    Map<String, Object> processedMap = new HashMap<>();
    for (Map.Entry<?, ?> entry : originalMap.entrySet()) {
        Object mapValue = entry.getValue();
        if (mapValue instanceof GenericRecord) {
            mapValue = deserializeComplexObject((GenericRecord) mapValue);
        }
        processedMap.put(entry.getKey().toString(), mapValue);
    }
    return processedMap;
}

// ... [rest of the deserializeComplexObject method]

private void invokeSetterMethod(Class<T> targetClass, T targetObject, String fieldName, Object value) {
    if (value == null) return;
    try {
        Method setter;
        if (value instanceof List) {
            setter = findListSetter(targetClass, fieldName);
        } else if (value instanceof Map) {
            setter = findMapSetter(targetClass, fieldName);
        } else {
            setter = targetClass.getMethod(buildSetterName(fieldName), value.getClass());
        }
        setter.invoke(targetObject, value);
    } catch (ReflectiveOperationException e) {
        System.err.println("Error handling setter for property: " + fieldName);
        e.printStackTrace();
    }
}

private Method findMapSetter(Class<?> targetClass, String fieldName) throws NoSuchMethodException {
    // Implement similar to findListSetter, but for Map
}

private String buildSetterName(String fieldName) {
    return "set" + Character.toUpperCase(fieldName.charAt(0)) + fieldName.substring(1);
}

// ... [rest of the class]
