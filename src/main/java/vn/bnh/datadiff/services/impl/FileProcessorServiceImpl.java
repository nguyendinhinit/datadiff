package vn.bnh.datadiff.services.impl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import vn.bnh.datadiff.services.FileProcessorService;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class FileProcessorServiceImpl implements FileProcessorService {
    Logger log4j = LogManager.getLogger(FileProcessorServiceImpl.class);

    /**
     * readPropertiesFile reads the properties file at the specified file path.
     * It loads the properties into a Properties object and returns it.
     * If there is an error reading the file, it logs the error and returns an empty Properties object.
     *
     * @param filePath The path to the properties file.
     *                 The properties file should include connection strings, usernames, passwords and database names of the source and destination databases.
     */
    @Override
    public Properties readPropertiesFile(String filePath) {
        log4j.info("Start reading file {}", filePath);
        Properties fileProperties = new Properties();
        try (FileInputStream fis = new FileInputStream(filePath)) {
            fileProperties.load(fis);
        } catch (IOException e) {
            log4j.error("Error reading file {}: {}", filePath, e.getMessage());
        }
        return fileProperties;
    }


    @Override
    public boolean FileChecker(String filePath) {

        return false;
    }
}