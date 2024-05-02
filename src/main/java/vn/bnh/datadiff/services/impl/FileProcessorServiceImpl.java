package vn.bnh.datadiff.services.impl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import vn.bnh.datadiff.dto.DBObject;
import vn.bnh.datadiff.services.FileProcessorService;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Properties;


public class FileProcessorServiceImpl implements FileProcessorService {
    Logger log4j = LogManager.getLogger(FileProcessorServiceImpl.class);

    Properties properties = new Properties();

    /**
     * readPropertiesFile reads the properties file at the specified file path.
     * It loads the properties into a Properties object and returns it.
     * If there is an error reading the file, it logs the error and returns an empty Properties object.
     *
     * @param file The path to the properties file.
     *             The properties file should include connection strings, usernames, passwords and database names of the source and destination databases.
     */
    @Override
    public Properties readPropertiesFile(File file) {
        String fileLocation = file.getAbsolutePath();
        log4j.info("Start reading file {} at {}", fileLocation);
        Properties fileProperties = new Properties();
        try (FileInputStream fis = new FileInputStream(file)) {
            fileProperties.load(fis);
        } catch (IOException e) {
            log4j.error("Error reading file {}: {}", file, e.getMessage());
            return null;
        }
        return fileProperties;
    }


    //TODO: Implement the FileChecker method
    @Override
    public boolean FileChecker(String filePath) {
        return false;
    }

    @Override
    public Properties readPropertiesFileV2(File file) {
        log4j.info("Start reading file {}", file);
        try {
            String filePath = String.valueOf(file.toPath());
            log4j.info("File path: {}", filePath);
            properties.load(Files.newInputStream(file.toPath()));
            return properties;
        } catch (IOException e) {
            e.printStackTrace();
            log4j.error("File not found: {}", file);
            return null;
        }
    }
}