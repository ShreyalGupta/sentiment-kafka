package org.example;

import java.io.FileInputStream;
import java.util.Properties;

public class Utils {
    private static Properties props = null;

    static {
        props = new Properties();
        try {
            props.load(new FileInputStream("config.properties"));
        } catch (Exception e) {
            // file might not exist; that's fine
        }
    }

    public static String getEnvOrConfig(String key, String defaultValue) {
        String v = System.getenv(key.toUpperCase().replace('.', '_'));
        if (v != null && !v.isBlank()) return v;
        v = props.getProperty(key);
        if (v != null && !v.isBlank()) return v;
        return defaultValue;
    }
}
