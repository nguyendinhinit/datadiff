package vn.bnh.datadiff.service;

import vn.bnh.datadiff.dto.ConfigFile;

import java.util.Properties;

public class FileReaderService {

    ConfigFile configFile = new ConfigFile();
    public Properties readPropertiesFile(String filePath) {
        Properties properties = new Properties();
        try {
            properties.load(getClass().getClassLoader().getResourceAsStream(filePath));
        } catch (Exception e) {
            e.printStackTrace();
        }
        // Read properties file
        return properties;
    }
}
