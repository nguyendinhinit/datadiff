package vn.bnh.datadiff.services.impl;

import lombok.extern.log4j.Log4j;
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
        log4j.info("Start reading file {}", filePath);
        //Check file
        File propertiesFile = new File(filePath);
        
        try{
            Properties fileProperties = new Properties();
            fileProperties.load(Files.newInputStream(Paths.get(filePath)));
            log4j.info("Load properties file succesfully");
            return fileProperties;
        }catch (Exception e){
            log4j.error("Application properties file not found");
        }
        return null;
    }


    @Override
    public boolean FileChecker(String filePath) {

        return false;
    }
}