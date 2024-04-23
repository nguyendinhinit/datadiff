package vn.bnh.datadiff.services;

import vn.bnh.datadiff.dto.DBObject;

import java.util.Properties;

public interface ObjectCreator {

    public DBObject create(String connectionString, String username, String password, String dbname);

    public DBObject create(Properties properties, String dbType);
}