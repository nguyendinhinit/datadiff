package vn.bnh.datadiff.services;

import vn.bnh.datadiff.dto.DbObject;

public interface ObjectCreator {
    
    public DbObject create(String connectionString, String username, String password, String dbname);
}