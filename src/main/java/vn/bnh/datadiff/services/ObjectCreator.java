package vn.bnh.datadiff.services;

import vn.bnh.datadiff.dto.DBObject;

public interface ObjectCreator {
    
    public DBObject create(String connectionString, String username, String password, String dbname);
}