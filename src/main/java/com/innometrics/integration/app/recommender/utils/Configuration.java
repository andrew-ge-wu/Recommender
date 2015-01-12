package com.innometrics.integration.app.recommender.utils;

import com.innometrics.utils.app.commons.settings.store.ContextSettings;
import org.apache.commons.configuration.ConfigurationException;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.util.List;
import java.util.Properties;

/**
 * @author andrew, Innometrics
 */
public class Configuration {
    public static String DEFAULT_CONFIG = "default.properties";
    private final ContextSettings delegate;
    private final String prefix;

    public Configuration(Class cName) throws ConfigurationException {
        this(cName, ClassLoader.getSystemResource(DEFAULT_CONFIG), ClassLoader.getSystemResource(cName.getCanonicalName() + ".properties"));
    }

    public Configuration(Class cName, URL... configFiles) throws ConfigurationException {
        delegate = new ContextSettings(configFiles);
        this.prefix = cName.getCanonicalName();
    }


    public Properties getProperties(String key) {
        return delegate.getProperties(prefix + "." + key);
    }


    public Properties getProperties(String key, Properties defaults) {
        return delegate.getProperties(prefix + "." + key, defaults);
    }


    public boolean getBoolean(String key) {
        return delegate.getBoolean(prefix + "." + key);
    }


    public boolean getBoolean(String key, boolean defaultValue) {
        return delegate.getBoolean(prefix + "." + key, defaultValue);
    }


    public Boolean getBoolean(String key, Boolean defaultValue) {
        return delegate.getBoolean(prefix + "." + key, defaultValue);
    }


    public byte getByte(String key) {
        return delegate.getByte(prefix + "." + key);
    }


    public byte getByte(String key, byte defaultValue) {
        return delegate.getByte(prefix + "." + key, defaultValue);
    }


    public Byte getByte(String key, Byte defaultValue) {
        return delegate.getByte(prefix + "." + key, defaultValue);
    }


    public double getDouble(String key) {
        return delegate.getDouble(prefix + "." + key);
    }


    public double getDouble(String key, double defaultValue) {
        return delegate.getDouble(prefix + "." + key, defaultValue);
    }


    public Double getDouble(String key, Double defaultValue) {
        return delegate.getDouble(prefix + "." + key, defaultValue);
    }


    public float getFloat(String key) {
        return delegate.getFloat(prefix + "." + key);
    }


    public float getFloat(String key, float defaultValue) {
        return delegate.getFloat(prefix + "." + key, defaultValue);
    }


    public Float getFloat(String key, Float defaultValue) {
        return delegate.getFloat(prefix + "." + key, defaultValue);
    }


    public int getInt(String key) {
        return delegate.getInt(prefix + "." + key);
    }


    public int getInt(String key, int defaultValue) {
        return delegate.getInt(prefix + "." + key, defaultValue);
    }


    public Integer getInteger(String key, Integer defaultValue) {
        return delegate.getInteger(prefix + "." + key, defaultValue);
    }


    public long getLong(String key) {
        return delegate.getLong(prefix + "." + key);
    }


    public long getLong(String key, long defaultValue) {
        return delegate.getLong(prefix + "." + key, defaultValue);
    }


    public Long getLong(String key, Long defaultValue) {
        return delegate.getLong(prefix + "." + key, defaultValue);
    }


    public short getShort(String key) {
        return delegate.getShort(prefix + "." + key);
    }


    public short getShort(String key, short defaultValue) {
        return delegate.getShort(prefix + "." + key, defaultValue);
    }


    public Short getShort(String key, Short defaultValue) {
        return delegate.getShort(prefix + "." + key, defaultValue);
    }


    public BigDecimal getBigDecimal(String key) {
        return delegate.getBigDecimal(prefix + "." + key);
    }


    public BigDecimal getBigDecimal(String key, BigDecimal defaultValue) {
        return delegate.getBigDecimal(prefix + "." + key, defaultValue);
    }


    public BigInteger getBigInteger(String key) {
        return delegate.getBigInteger(prefix + "." + key);
    }


    public BigInteger getBigInteger(String key, BigInteger defaultValue) {
        return delegate.getBigInteger(prefix + "." + key, defaultValue);
    }


    public String getString(String key) {
        return delegate.getString(prefix + "." + key);
    }


    public String getString(String key, String defaultValue) {
        return delegate.getString(prefix + "." + key, defaultValue);
    }


    public List<Object> getList(String key) {
        return delegate.getList(prefix + "." + key);
    }
}
