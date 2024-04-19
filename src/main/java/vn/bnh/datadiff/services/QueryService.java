package vn.bnh.datadiff.services;

import vn.bnh.datadiff.dto.DBObject;
import vn.bnh.datadiff.dto.TableObject;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

public interface QueryService {
    public ArrayList<String> getSchemaList(DBObject dbObject);
    public ArrayList<String> getSchemaList(DBObject dbObject, String query);

    public ArrayList<String> getTableList(DBObject dbObject, String schema);

    public ArrayList<TableObject> getTableMetadata(DBObject dbOject, String tableName, String schemaName);
    public LinkedHashMap<String, Map<String, ArrayList<String>>> getDbMetadata(DBObject dbObject, ArrayList<String> schemaList);
    public LinkedHashMap<String, Map<String, ArrayList<TableObject>>> getDbMetadata(DBObject dbObject);

}