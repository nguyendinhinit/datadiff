package vn.bnh.datadiff.controllers;

import vn.bnh.datadiff.dto.DBObject;
import vn.bnh.datadiff.dto.ColumnObject;
import vn.bnh.datadiff.services.QueryService;
import vn.bnh.datadiff.services.impl.QueryServiceImpl;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

public class QueryController{
    QueryService queryService = new QueryServiceImpl();

    public ArrayList<String> getSchema(DBObject dbObject) {
        return queryService.getSchemaList(dbObject);
    }

    public ArrayList<String> getSchema(DBObject dbObject, String query) {
        return queryService.getSchemaList(dbObject, query);
    }

    public ArrayList<String> getTable(DBObject dbObject, String schema) {
        return queryService.getTableList(dbObject, schema);
    }


    public LinkedHashMap<String, Map<String, ArrayList<String>>> getDbMetadata(DBObject dbObject, ArrayList<String> schemaList){
        return queryService.getDbMetadata(dbObject, schemaList);
    }

    public  LinkedHashMap<String, Map<String, ArrayList<ColumnObject>>> getDbMetadata(DBObject dbObject){
        return queryService.getDbMetadata(dbObject);
    }

    public  LinkedHashMap<String, Map<String, ArrayList<ColumnObject>>> getDbMetadata(DBObject dbObject, String query){
        return queryService.getDbMetadata(dbObject, query);
    }

    public Map<String, Map<String, ArrayList<Integer>>> getObjectMetadata(DBObject dbObject){
        return queryService.getObjectMetadata(dbObject);
    }
}