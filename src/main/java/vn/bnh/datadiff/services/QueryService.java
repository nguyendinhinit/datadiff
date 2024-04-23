package vn.bnh.datadiff.services;

import vn.bnh.datadiff.dto.ColumnObject;
import vn.bnh.datadiff.dto.DBObject;

import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

public interface QueryService {
    ArrayList<String> getSchemaList(DBObject dbObject);

    ArrayList<String> getSchemaList(DBObject dbObject, String query);

    ArrayList<String> getTableList(DBObject dbObject, String schema);

    LinkedHashMap<String, Map<String, ArrayList<String>>> getDbMetadata(DBObject dbObject, ArrayList<String> schemaList);

    LinkedHashMap<String, Map<String, ArrayList<ColumnObject>>> getDbMetadata(DBObject dbObject);

    LinkedHashMap<String, Map<String, ArrayList<ColumnObject>>> getDbMetadata(DBObject dbObject, String query);

    ArrayList<ColumnObject> getColumnMetadata(DBObject dbObject, String tableName, String schemaName, ArrayList<String> pKs, ArrayList<String> incremental);

    public Map<String, Integer[]> countConstraintsAndIndexes(DBObject dbObject);

    public <T> T queryResult(Class<T> returnType);

    public Integer queryResult(Statement statement, String query, String schemaName, String tableName, String columnName);

    public Map<String, Map<String, ArrayList<Integer>>> getObjectMetadata(DBObject dbObject);
}