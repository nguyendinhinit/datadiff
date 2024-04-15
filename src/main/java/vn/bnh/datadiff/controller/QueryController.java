package vn.bnh.datadiff.controller;

import com.github.benmanes.caffeine.cache.Cache;
import org.json.JSONObject;
import vn.bnh.datadiff.dto.DBObject;
import vn.bnh.datadiff.service.QueryService;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

public class QueryController {
    QueryService queryService = new QueryService();

    public ArrayList<String> getTableList(Statement statement, String[] schemaList, Cache cache, String query, String database) throws SQLException {
        return queryService.getTableList(statement, schemaList, cache, query, database);
    }

    public JSONObject getTableMetadata(String query, Statement statement, String database) throws SQLException {
        return queryService.getTableMetadata(query, statement, database);
    }

    public JSONObject getSchemaMetaData(DBObject object, Statement statement, String database, String query,ArrayList<String> tableList, ArrayList<String> pK) throws SQLException {
        return queryService.getSchemaMetaData(object, statement, database, query, tableList, pK);
    }


    public ArrayList<String> findPk(Statement statement, String  tableName, String schemaName) throws SQLException {
        return queryService.findPk(statement, tableName, schemaName);
    }

    public ArrayList<String> countJob(Statement statement, String schemaName, String database) throws SQLException {
        return queryService.countJob(statement, schemaName,database);
    }
}
