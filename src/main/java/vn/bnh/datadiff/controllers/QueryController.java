package vn.bnh.datadiff.controllers;

import vn.bnh.datadiff.dto.DBObject;
import vn.bnh.datadiff.services.QueryService;
import vn.bnh.datadiff.services.impl.QueryServiceImpl;

import java.util.ArrayList;

public class QueryController {
    QueryService queryService = new QueryServiceImpl();

    public ArrayList<String> getSchema(DBObject dbObject) {
        return queryService.getSchema(dbObject);
    }
    
    public ArrayList<String> getSchema(DBObject dbObject, String query){
        return queryService.getSchema(dbObject, query);
    }
    public ArrayList<String> getTable(DBObject dbObject, String schema){
        return queryService.getTable(dbObject,schema);
    }
}