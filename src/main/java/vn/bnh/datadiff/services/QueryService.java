package vn.bnh.datadiff.services;

import vn.bnh.datadiff.dto.DbObject;

import java.util.ArrayList;

public interface QueryService {
    public ArrayList<String> getSchema(DbObject dbObject); 
}