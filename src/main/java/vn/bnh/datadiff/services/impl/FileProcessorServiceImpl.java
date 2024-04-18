package vn.bnh.datadiff.services.impl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import vn.bnh.datadiff.services.FileProcessorService;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

public class FileProcessorServiceImpl implements FileProcessorService {
    Logger log4j = LogManager.getLogger(FileProcessorServiceImpl.class);

    @Override
    public Properties readPropertiesFile(String filePath) {
        //Check file
        File propertiesFile = new File(filePath);
        
        try{
            Properties fileProperties = new Properties();
            fileProperties.load(Files.newInputStream(Paths.get(filePath)));
            return fileProperties;
        }catch (Exception e){
            log4j.error("Application properties file not found");
        }
        return null;
    }
}