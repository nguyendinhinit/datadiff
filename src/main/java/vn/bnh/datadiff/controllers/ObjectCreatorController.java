package vn.bnh.datadiff.controllers;

import vn.bnh.datadiff.dto.DbObject;
import vn.bnh.datadiff.services.ObjectCreator;
import vn.bnh.datadiff.services.impl.ObjectCreatorImpl;

public class ObjectCreatorController {
    ObjectCreator objectCreator = new ObjectCreatorImpl();
    public DbObject create(String connectionString, String username, String password, String dbname){
        return new DbObject(connectionString,username,password,dbname);
    }
}