package vn.bnh.datadiff.controllers;

import vn.bnh.datadiff.dto.DBObject;
import vn.bnh.datadiff.services.ObjectCreator;
import vn.bnh.datadiff.services.impl.ObjectCreatorImpl;

import java.util.Properties;

public class ObjectCreatorController {
    ObjectCreator objectCreator = new ObjectCreatorImpl();
    public DBObject create(String connectionString, String username, String password, String dbname){
        return objectCreator.create(connectionString,username,password,dbname);
    }

    public DBObject create(Properties properties, String dbType){
        return objectCreator.create(properties,dbType);
    }
}