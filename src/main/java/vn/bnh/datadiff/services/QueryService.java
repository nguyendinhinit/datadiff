package vn.bnh.datadiff.services;

import vn.bnh.datadiff.dto.DBObject;

import java.util.ArrayList;

public interface QueryService {
    public ArrayList<String> getSchema(DBObject dbObject);
    public ArrayList<String> getSchema(DBObject dbObject, String query);

    public ArrayList<String> getTable(DBObject dbObject, String schema);
}