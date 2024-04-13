package org.example;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class FileReader {
      Properties readPropertiesFile(String fileName) throws IOException {
        Properties properties = new Properties();
        try (InputStream inputStream = Main.class.getClassLoader().getResourceAsStream(fileName)) {
            properties.load(inputStream);
        }
        return properties;
    }
}
