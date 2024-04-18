package vn.bnh.datadiff.services.impl;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import vn.bnh.datadiff.dto.DbObject;
import vn.bnh.datadiff.services.ObjectCreator;

public class ObjectCreatorImpl implements ObjectCreator {

    Logger log4j = LogManager.getLogger(ObjectCreatorImpl.class);
    @Override
    public DbObject create(String connectionString, String username, String password, String dbname) {
        log4j.info("Creating {} object", dbname);
        return new DbObject(connectionString,username,password,dbname);
    }
}