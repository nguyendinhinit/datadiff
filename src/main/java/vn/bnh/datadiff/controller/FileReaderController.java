package vn.bnh.datadiff.controller;

import vn.bnh.datadiff.dto.DBObject;
import vn.bnh.datadiff.service.FileReaderService;

import java.util.Properties;

public class FileReaderController {
    FileReaderService fileReaderService = new FileReaderService();
    public Properties readFilePath(String filePath) {
        // Read file path
        return  fileReaderService.readPropertiesFile(filePath);
    }

    public DBObject createOracleObject(Properties properties) {
        // Create Oracle object
        return fileReaderService.createOracleObject(properties);
    }

    public DBObject createMysqlObject(Properties properties) {
        // Create Mysql object
        return fileReaderService.createMysqlObject(properties);
    }
}
