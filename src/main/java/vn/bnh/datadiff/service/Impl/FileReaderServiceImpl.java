package vn.bnh.datadiff.service.Impl;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import vn.bnh.datadiff.dto.DBObject;
import vn.bnh.datadiff.service.FileReaderService;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class FileReaderServiceImpl implements FileReaderService {
    Logger log4j = LogManager.getLogger(FileReaderServiceImpl.class);
    FileInputStream fileInputStream;

    @Override
    public Properties readPropertiesFile(String filePath) {
        log4j.info("Start reading properties file: " + filePath);
        String currentPath = System.getProperty("user.dir");
        log4j.info("Current path: " + currentPath);
        Properties properties = new Properties();
        try {
            fileInputStream = new FileInputStream(filePath);
            properties.load(fileInputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        log4j.info("Properties file read successfully");
        return properties;
    }
    
    @Override
    public DBObject createOracleObject(Properties properties) {
        DBObject oracleObject = new DBObject();
        oracleObject.setDatabase("oracle");
        oracleObject.setConnectionString(properties.getProperty("oracle_connection_string"));
        oracleObject.setUserName(properties.getProperty("oracle_username"));
        oracleObject.setPassword(properties.getProperty("oracle_password"));
        oracleObject.setSchemaList(properties.getProperty("table_schema").split(" "));
        return oracleObject;
    }

    @Override
    public DBObject createMysqlObject(Properties properties) {
        DBObject mysqlObject = new DBObject();
        mysqlObject.setDatabase("mysql");
        mysqlObject.setConnectionString(properties.getProperty("mysql_connection_string"));
        mysqlObject.setUserName(properties.getProperty("mysql_username"));
        mysqlObject.setPassword(properties.getProperty("mysql_password"));
        mysqlObject.setSchemaList(properties.getProperty("table_schema").split(" "));
        return mysqlObject;
    }
}
