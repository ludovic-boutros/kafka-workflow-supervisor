package com.example.demo.utils;

import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

/**
 * Hold the static context for the whole stream
 */
public class StreamContext {
    private static Storage STORAGE_INSTANCE;

    public static String get(String key) {
        if (STORAGE_INSTANCE == null) {
            throw new IllegalStateException("StreamContext is not initialized");
        }
        return STORAGE_INSTANCE.get(key);
    }

    public static Properties getAsProperties() {
        if (STORAGE_INSTANCE == null) {
            throw new IllegalStateException("StreamContext is not initialized");
        }
        return STORAGE_INSTANCE.getAsProperties();
    }

    public static void setEnvironment(ConfigurableEnvironment environment) {
        if (STORAGE_INSTANCE != null) {
            throw new IllegalStateException("StreamContext is static and can be initialized only once");
        }
        STORAGE_INSTANCE = new EnvironmentStorage(environment);
    }

    public static void setProperties(Properties properties) {
        if (STORAGE_INSTANCE != null) {
            throw new IllegalStateException("StreamContext is static and can be initialized only once");
        }
        STORAGE_INSTANCE = new PropertyStorage(properties);
    }

    private interface Storage {
        String get(String key);

        Properties getAsProperties();
    }

    private static class EnvironmentStorage implements Storage {
        private final ConfigurableEnvironment environment;

        private EnvironmentStorage(ConfigurableEnvironment environment) {
            this.environment = environment;
        }

        @Override
        public String get(String key) {
            return environment.getProperty(key);
        }

        @Override
        public Properties getAsProperties() {
            Properties retValue = new Properties();
            // Two phase iteration in order to decrypt secrets if any
            List<String> propertyNames = environment.getPropertySources().stream().map(propertySource -> {
                        if (propertySource instanceof MapPropertySource) {
                            return Arrays.stream(((MapPropertySource) propertySource).getPropertyNames()).toList();
                        }
                        return List.<String>of();
                    })
                    .flatMap(Collection::stream)
                    .toList();

            propertyNames.forEach(property -> retValue.put(property, environment.getProperty(property)));

            return retValue;
        }


    }

    private static class PropertyStorage implements Storage {
        private final Properties properties;

        private PropertyStorage(Properties properties) {
            this.properties = properties;
        }

        @Override
        public String get(String key) {
            return properties.getProperty(key);
        }

        @Override
        public Properties getAsProperties() {
            return new Properties(properties);
        }
    }
}