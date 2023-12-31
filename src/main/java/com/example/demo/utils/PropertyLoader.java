package com.example.demo.utils;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

public class PropertyLoader {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    /**
     * Return a Properties object from a Yaml file
     *
     * @param fileNameFromRessource The file path to load (relative to ressource folder)
     * @return filled Properties object
     */
    public static Properties fromYaml(String fileNameFromRessource) {
        Yaml yaml = new Yaml();
        InputStream inputStream = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream(fileNameFromRessource);
        Map<String, Object> obj = yaml.load(inputStream);

        var props = new Properties();
        exploreYamlObject(obj, props, "");

        return props;
    }

    public static <T> T fromYaml(String fileNameFromRessource, String property, Class<T> type) {
        Yaml yaml = new Yaml();
        InputStream inputStream = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream(fileNameFromRessource);
        Map<String, Object> obj = yaml.load(inputStream);
        return MAPPER.convertValue(obj.get(property), type);
    }

    /**
     * Fill Properties object with values from Yaml object
     *
     * @param obj         the Yaml structure to explore
     * @param properties  the current built properties object
     * @param currentPath the current path to the currently explored yaml branch
     * @return Properties object fill with child yaml attribute with complete path from root
     */
    @SuppressWarnings("unchecked")
    private static Properties exploreYamlObject(Map<String, Object> obj, Properties properties, String currentPath) {
        obj.keySet().forEach(key -> {

            var currentVal = obj.get(key);

            if (currentVal instanceof Map) { // Explore deeper
                if (currentPath.isEmpty()) {
                    exploreYamlObject((Map<String, Object>) currentVal, properties, key);
                } else {
                    exploreYamlObject((Map<String, Object>) currentVal, properties, currentPath + "." + key);
                }
            } else { // create child properties
                if (currentPath.isEmpty()) {
                    properties.put(key, currentVal);
                } else {
                    properties.put(currentPath + "." + key, currentVal);
                }
            }
        });

        return properties;
    }
}