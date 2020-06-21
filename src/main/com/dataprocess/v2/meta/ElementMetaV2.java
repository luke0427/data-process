package com.ropeok.dataprocess.v2.meta;

import java.util.HashMap;
import java.util.Map;

public abstract class ElementMetaV2 {
    private String clazz;
    private String id;
    private String name;
    protected Map<String, Object> properties = new HashMap<>();

    public void initProperties() {
        this.id = getStringProperty("id");
        this.name = getStringProperty("name");
        this.clazz = getStringProperty("class");
    }

    public String getClazz() {
        return clazz;
    }

    public void setClazz(String clazz) {
        this.clazz = clazz;
        setProperty("class", clazz);
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
        setProperty("id", id);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
        setProperty("name", name);
    }

    public void setProperty(String name, Object object) {
        properties.put(name, object);
    }

    public Object getProperty(String name) {
        return properties.get(name);
    }

    public String getStringProperty(String name) {
        return (String) properties.get(name);
    }

    public int getIntProperty(String name) {
        return Integer.parseInt((String) properties.get(name));
    }

    public int getOrDefaultIntProperty(String name, int defaultValue) {
        return Integer.parseInt(String.valueOf(properties.getOrDefault(name, defaultValue)));
    }

    public boolean getBooleanProperty(String name) {
        return Boolean.parseBoolean((String) properties.get(name));
    }

    public boolean getOrDefaultBooleanProperty(String name, boolean defaultValue) {
        return (boolean) properties.getOrDefault(name, defaultValue);
    }

    public String getOrDefaultStringProperty(String name, String defaultValue) {
        return (String) properties.getOrDefault(name, defaultValue);
    }

    public String[] getStringPropertyToArray(String name) {
        String value = getStringProperty(name);
        return value != null ? value.split(",") : null;
    }
}
