package vn.bnh.datadiff.services;

import vn.bnh.datadiff.dto.DBObject;

import java.util.Properties;

public interface ObjectCreator {

    DBObject create(String connectionString, String username, String password, String dbname);

    DBObject create(Properties properties, String dbType);
}