package vn.bnh.datadiff.service;

import com.github.benmanes.caffeine.cache.Cache;
import lombok.extern.log4j.Log4j;
import org.json.JSONObject;
import vn.bnh.datadiff.dto.DBObject;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Locale;
import java.util.Objects;
import java.util.logging.Logger;

public class QueryService {

    Logger log4j = Logger.getLogger(QueryService.class.getName());


    public ArrayList<String> getTableList(Statement statement, String[] databaseSchema, Cache cache, String query, String database) throws SQLException {
        DBObject object = (DBObject) cache.getIfPresent(database);
        log4j.info("Getting table list from database " + object.getDatabase().toUpperCase(Locale.ROOT) + " with schema: " + Arrays.toString(databaseSchema));
        log4j.info("Executing query: " + query + "'" + databaseSchema[0].toUpperCase(Locale.ROOT) + "'");
        ResultSet resultSet = statement.executeQuery(query + "'" + databaseSchema[0].toUpperCase(Locale.ROOT) + "'");

        log4j.info("Executing query: " + query + "'" + databaseSchema[0] + "'");
        ArrayList<String> tableList = new ArrayList<>();
        while (resultSet.next()) {
            String tableName = resultSet.getString("table_name");
            tableList.add(tableName);
        }
        return tableList;
    }

    public JSONObject getTableMetadata(String query, Statement statement, String database) {
        JSONObject metadata = new JSONObject();
        switch (database) {
            case "mysql":
                try {
                    ResultSet resultSet = statement.executeQuery(query);
                    while (resultSet.next()) {
                        String tableName = resultSet.getString("TABLE_SCHEMA");
                        String columnName = resultSet.getString("TABLE_NAME");
                        String dataType = resultSet.getString("COLUMN_NAME");
                        String columnKey = resultSet.getString("COLUMN_KEY");
                        String isNullable = resultSet.getString("IS_NULLABLE");
                        String columnType = resultSet.getString("COLUMN_TYPE");
                        metadata.put("tableName", tableName);
                        metadata.put("columnName", columnName);
                        metadata.put("dataType", dataType);
                        metadata.put("columnKey", columnKey);
                        metadata.put("isNullable", isNullable);
                        metadata.put("columnType", columnType);
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                break;
            case "oracle":
//                metadata = getOracleTableMetadata(query, statement);
                break;
        }
        return metadata;
    }
}
