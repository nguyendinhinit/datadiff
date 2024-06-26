package vn.bnh.datadiff.service;


import vn.bnh.datadiff.dto.DBObject;

import java.io.FileInputStream;
import java.util.Properties;
import java.util.logging.Logger;

public class FileReaderService {
    Logger log4j = Logger.getLogger(FileReaderService.class.getName());

    FileInputStream fileInputStream;

    public Properties readPropertiesFile(String filePath) {
        log4j.info("Start reading properties file: " + filePath);
        String currentPath = System.getProperty("user.dir");
        log4j.info("Current path: " + currentPath);
        Properties properties = new Properties();
        try {
            fileInputStream = new FileInputStream(filePath);
            properties.load(fileInputStream);
        } catch (Exception e) {
            e.printStackTrace();
        }
        log4j.info("Properties file read successfully");
        return properties;
    }
    public DBObject createOracleObject(Properties properties) {
        DBObject oracleObject = new DBObject();
        oracleObject.setDatabase("oracle");
        oracleObject.setConnectionString(properties.getProperty("oracle_connection_string"));
        oracleObject.setUserName(properties.getProperty("oracle_username"));
        oracleObject.setPassword(properties.getProperty("oracle_password"));
        oracleObject.setSchemaList(properties.getProperty("table_schema").split(" "));
        return oracleObject;
    }

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
