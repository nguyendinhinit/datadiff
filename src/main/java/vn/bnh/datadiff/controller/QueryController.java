package vn.bnh.datadiff.controller;

import org.json.JSONObject;
import vn.bnh.datadiff.dto.DBObject;
import vn.bnh.datadiff.service.Impl.QueryServiceImpl;

import java.io.FileNotFoundException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Map;

public class QueryController {
    QueryServiceImpl queryServiceImpl = new QueryServiceImpl();

    public ArrayList<String> getTableList(String connectionString, String database, String username, String password, String schema) {
        return queryServiceImpl.getTableList(connectionString, database, username, password, schema);
    }

    public JSONObject getTableMetadata(String query, Statement statement, String database) throws SQLException {
        return queryServiceImpl.getTableMetadata(query, statement, database);
    }

    public JSONObject getSchemaMetaData(DBObject object, Statement statement, String database, String query, ArrayList<String> tableList, ArrayList<String> pK) throws SQLException {
        return queryServiceImpl.getSchemaMetaData(object, statement, database, query, tableList, pK);
    }


    public ArrayList<String> findPk(Statement statement, String tableName, String schemaName) throws SQLException {
        return queryServiceImpl.findPk(statement, tableName, schemaName);
    }

    public Map<String, String> countJob(Statement statement, String schemaName, String database) throws SQLException {
        return queryServiceImpl.countJob(statement, schemaName, database);
    }

    public void getAllSchema(String query, String connectionString, String databaseType, String userName, String passWord) {
        queryServiceImpl.getAllSchema(query, connectionString, databaseType, userName, passWord);
    }
}
