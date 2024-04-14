package vn.bnh.datadiff.service;


import vn.bnh.datadiff.dto.MysqlObject;
import vn.bnh.datadiff.dto.OracleObject;

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
    public OracleObject createOracleObject(Properties properties) {
        OracleObject oracleObject = new OracleObject();
        oracleObject.setOracleConnection(properties.getProperty("oracle_connection_string"));
        oracleObject.setOracleUser(properties.getProperty("oracle_username"));
        oracleObject.setOraclePassword(properties.getProperty("oracle_password"));
        oracleObject.setSchemaList(properties.getProperty("table_schema").split(" "));
        return oracleObject;
    }

    public MysqlObject createMysqlObject(Properties properties) {
        MysqlObject mysqlObject = new MysqlObject();
        mysqlObject.setMysqlConnection(properties.getProperty("mysql_connection_string"));
        mysqlObject.setMysqlUser(properties.getProperty("mysql_username"));
        mysqlObject.setMysqlPassword(properties.getProperty("mysql_password"));
        mysqlObject.setSchemaList(properties.getProperty("table_schema").split(" "));
        return mysqlObject;
    }
}
