package vn.bnh.datadiff.service;

import vn.bnh.datadiff.dto.DBObject;

import java.util.Properties;

public interface FileReaderService {
    public Properties readPropertiesFile(String filePath);

    public DBObject createOracleObject(Properties properties);

    public DBObject createMysqlObject(Properties properties);
}
