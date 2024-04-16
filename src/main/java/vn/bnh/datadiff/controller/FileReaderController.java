package vn.bnh.datadiff.controller;

import vn.bnh.datadiff.dto.DBObject;
import vn.bnh.datadiff.service.FileReaderService;
import vn.bnh.datadiff.service.Impl.FileReaderServiceImpl;

import java.util.Properties;

public class FileReaderController {
    FileReaderService fileReaderServiceImpl = new FileReaderServiceImpl();
    public Properties readPropertiesFile(String filePath) {
        // Read file path
        return  fileReaderServiceImpl.readPropertiesFile(filePath);
    }

    public DBObject createOracleObject(Properties properties) {
        // Create Oracle object
        return fileReaderServiceImpl.createOracleObject(properties);
    }

    public DBObject createMysqlObject(Properties properties) {
        // Create Mysql object
        return fileReaderServiceImpl.createMysqlObject(properties);
    }
}
