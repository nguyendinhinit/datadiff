package vn.bnh.datadiff.service;

import com.github.benmanes.caffeine.cache.Cache;
import lombok.extern.log4j.Log4j;
import org.json.JSONArray;
import org.json.JSONObject;
import vn.bnh.datadiff.controller.DatabaseController;
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

    DatabaseService databaseService = new DatabaseService();

    public ArrayList<String> getTableList(Statement statement, String[] databaseSchema, Cache cache, String query, String database) throws SQLException {
        DBObject object = (DBObject) cache.getIfPresent(database);
        log4j.info("Getting table list from database " + object.getDatabase().toUpperCase(Locale.ROOT) + " with schema: " + Arrays.toString(databaseSchema));
        ArrayList<String> tableList = new ArrayList<>();
        int tableCount = 0;


        for (String schema : databaseSchema) {
            Statement statement1 = databaseService.connectToDatabase(object);
            ResultSet resultSet = statement1.executeQuery(query + "'" + schema.toUpperCase(Locale.ROOT) + "'");
            log4j.info("Executing query: " + query + "'" + schema.toUpperCase(Locale.ROOT) + "'");
            if (resultSet != null) {
                while (resultSet.next()) {
                    String tableName = resultSet.getString("TABLE_NAME");
                    tableList.add(schema.toUpperCase() + "." + tableName.toUpperCase());
                    tableCount++;
                }
            }
            resultSet.close();
            log4j.info("Get " + tableCount + " table from " + schema + " schema of database " + object.getDatabase().toUpperCase(Locale.ROOT));
        }

        return tableList;
    }

    public JSONObject getTableMetadata(String query, Statement statement, String database) {
        JSONObject metadata = new JSONObject();
        JSONArray columnArray = new JSONArray();
        String tableName = null;
        switch (database) {
            case "mysql":
                try {
                    ResultSet resultSet = statement.executeQuery(query);
                    while (resultSet.next()) {
                        JSONObject columnInfo = new JSONObject();
                        String tableSchema = resultSet.getString("TABLE_SCHEMA");
                        tableName = resultSet.getString("TABLE_NAME");
                        String columnName = resultSet.getString("COLUMN_NAME");
                        String isNullable = resultSet.getString("IS_NULLABLE");
                        String dataType = resultSet.getString("DATA_TYPE");
                        String characterMaximumLength = resultSet.getString("CHARACTER_MAXIMUM_LENGTH");
                        String numericPrecision = resultSet.getString("NUMERIC_PRECISION");
                        String numericScale = resultSet.getString("NUMERIC_SCALE");
                        String columnKey = resultSet.getString("COLUMN_KEY");
                        String columnDefault = resultSet.getString("COLUMN_DEFAULT");

                        columnInfo.put("TABLE_SCHEMA", tableSchema);
                        columnInfo.put("TABLE_NAME", tableName);
                        columnInfo.put("COLUMN_NAME", columnName);
                        columnInfo.put("IS_NULLABLE", isNullable);
                        columnInfo.put("DATA_TYPE", dataType);
                        if (characterMaximumLength != null)
                            columnInfo.put("CHARACTER_MAXIMUM_LENGTH", characterMaximumLength);
                        else
                            columnInfo.put("CHARACTER_MAXIMUM_LENGTH", "NULL");


                        if (numericPrecision != null)
                            columnInfo.put("NUMERIC_PRECISION", numericPrecision);
                        else
                            columnInfo.put("NUMERIC_PRECISION", "NULL");


                        if (numericScale != null)
                            columnInfo.put("NUMERIC_SCALE", numericScale);
                        else
                            columnInfo.put("NUMERIC_SCALE", "NULL");

                        if (columnDefault != null)
                            columnInfo.put("COLUMN_DEFAULT", columnDefault);
                        else
                            columnInfo.put("COLUMN_DEFAULT", "NULL");

                        columnInfo.put("COLUMN_KEY", columnKey);
                        columnArray.put(columnInfo);
                    }
                    if (tableName != null) metadata.put(tableName, columnArray);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                break;
            case "oracle":
                try {
                    ResultSet resultSet = statement.executeQuery(query);
                    while (resultSet.next()) {
                        JSONObject columnInfo = new JSONObject();
                        String tableSchema = resultSet.getString("OWNER");
                        tableName = resultSet.getString("TABLE_NAME");
                        String columnName = resultSet.getString("COLUMN_NAME");
                        String columnDataType = resultSet.getString("DATA_TYPE");
                        String columnDataLength = resultSet.getString("DATA_LENGTH");
                        String columnDataPrecision = resultSet.getString("DATA_PRECISION");
                        String columnDataScale = resultSet.getString("DATA_SCALE");
                        String nullable = resultSet.getString("NULLABLE");
                        String columnDefault = resultSet.getString("DATA_DEFAULT");
//                        String
                        columnInfo.put("TABLE_SCHEMA", tableSchema);
                        columnInfo.put("TABLE_NAME", tableName);
                        columnInfo.put("COLUMN_NAME", columnName);
                        columnInfo.put("DATA_TYPE", columnDataType);
                        columnInfo.put("CHARACTER_MAXIMUM_LENGTH", columnDataLength);

                        if (columnDataPrecision != null)
                            columnInfo.put("NUMERIC_PRECISION", columnDataPrecision);
                        else
                            columnInfo.put("NUMERIC_PRECISION", "NULL");

                        if (columnDataScale != null)
                            columnInfo.put("NUMERIC_SCALE", columnDataScale);
                        else
                            columnInfo.put("NUMERIC_SCALE", "NULL");

                        columnInfo.put("IS_NULLABLE", nullable);
                        columnInfo.put("COLUMN_KEY", "N/A");

                        if (columnDefault != null)
                            columnInfo.put("COLUMN_DEFAULT", columnDefault);
                        else
                            columnInfo.put("COLUMN_DEFAULT", "NULL");

                        columnArray.put(columnInfo);
                    }
                    if (tableName != null) metadata.put(tableName, columnArray);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                break;
        }
        return metadata;
    }

    public JSONObject getSchemaMetaData(DBObject dbObject, Statement statement, String database, String query, ArrayList<String> tableList) {
        JSONObject metadata = new JSONObject();
        JSONArray tableArray = new JSONArray();
        switch (database) {
            case "mysql":
                for (String schema : dbObject.getSchemaList()) {
                    for (String table : tableList) {
                        String tableName = table.substring(table.lastIndexOf(".") + 1);
                        String mysqlMetadataQuery = query + " TABLE_SCHEMA = '" + schema + "'" + " AND TABLE_NAME = '" + tableName + "'";
                        log4j.info("Getting metadata for table " + tableName + " in schema " + schema);
                        JSONObject mysqlTableMetadata = new JSONObject();
                        mysqlTableMetadata = getTableMetadata(mysqlMetadataQuery, statement, database);
                        tableArray.put(mysqlTableMetadata);
                    }
                    metadata.put(schema, tableArray);
                }
                return metadata;
            case "oracle":
                for (String schema : dbObject.getSchemaList()) {
                    for (String table : tableList) {
                        String tableName = table.substring(table.lastIndexOf(".") + 1);
                        String oracleMetadataQuery = query + schema.toUpperCase() + "' AND TABLE_NAME = '" + tableName + "'";
                        log4j.info("Getting metadata for table " + tableName + " in schema " + schema);
                        JSONObject oracleTableMetadata = new JSONObject();
                        oracleTableMetadata = getTableMetadata(oracleMetadataQuery, statement, "oracle");
                        tableArray.put(oracleTableMetadata);
                    }
                    metadata.put(schema, tableArray);
                }
                return metadata;
        }
        return null;
    }
}