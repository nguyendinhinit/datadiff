package vn.bnh.datadiff.services.impl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import vn.bnh.datadiff.dto.DBObject;
import vn.bnh.datadiff.services.ObjectCreator;

import java.util.Properties;

public class ObjectCreatorImpl implements ObjectCreator {

    Logger log4j = LogManager.getLogger(ObjectCreatorImpl.class);

    @Override
    public DBObject create(String connectionString, String username, String password, String dbname) {
        log4j.info("Creating DBO: {}", dbname);

        return new DBObject(connectionString, username, password, dbname);
    }

    public DBObject create(Properties properties, String dbType) {
        String connectionString = properties.getProperty(dbType + "_connection_string");
        String username = properties.getProperty(dbType + "_username");
        String password = properties.getProperty(dbType + "_password");
        String dbname = properties.getProperty(dbType + "_dbname");
        return create(connectionString, username, password, dbname);
    }
}