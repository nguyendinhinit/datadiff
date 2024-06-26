package vn.bnh.datadiff.service;

import com.github.benmanes.caffeine.cache.Cache;
import lombok.extern.log4j.Log4j;
import org.json.JSONArray;
import org.json.JSONObject;
import vn.bnh.datadiff.controller.DatabaseController;
import vn.bnh.datadiff.dto.DBObject;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Stream;

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
//            resultSet.close();
            log4j.info("Get " + tableCount + " table from " + schema + " schema of database " + object.getDatabase().toUpperCase(Locale.ROOT));
        }

        return tableList;
    }

    public JSONObject getTableMetadata(String query, Statement statement, String database) throws SQLException {
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

    public JSONObject getTableMetadata(String query, Statement statement, String database, ArrayList<String> pK) throws SQLException {
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

                        Stream pKStream = pK.stream();
                        if (pKStream.anyMatch(columnName::equals))
                            columnInfo.put("COLUMN_KEY", "PRI");
                        else
                            columnInfo.put("COLUMN_KEY", "");

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

    public JSONObject getSchemaMetaData(DBObject dbObject, Statement statement, String database, String query, ArrayList<String> tableList, ArrayList<String> pK) throws SQLException {
        JSONObject metadata = new JSONObject();
        JSONArray tableArray = new JSONArray();
        switch (database) {
            case "mysql":
                for (String schema : dbObject.getSchemaList()) {
                    for (String table : tableList) {
                        String tableName = table.substring(table.lastIndexOf(".") + 1);
                        String mysqlMetadataQuery = query + " TABLE_SCHEMA = '" + schema + "'" + " AND TABLE_NAME = '" + tableName + "' ORDER BY COLUMN_NAME collate utf8_bin";
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
                        String oracleMetadataQuery = query + schema.toUpperCase() + "' AND TABLE_NAME = '" + tableName + "'  ORDER BY COLUMN_NAME ASC";
                        log4j.info("Getting metadata for table " + tableName + " in schema " + schema);
                        JSONObject oracleTableMetadata;
                        oracleTableMetadata = getTableMetadata(oracleMetadataQuery, statement, "oracle", pK);
                        tableArray.put(oracleTableMetadata);
                    }
                    metadata.put(schema, tableArray);
                }
                return metadata;
        }
        return null;
    }

    public Map<String, String> countJob(Statement statement, String schemaName, String database) throws SQLException {
        Map<String, String> countJob = new HashMap<>();
        switch (database) {
            case "mysql":
                String queryCountJob = "SELECT SCHEMA_NAME, IFNULL(NoTABLE, 0) AS NoTABLE, IFNULL(NoVIEW, 0) AS NoVIEW, IFNULL(NoTRIGGER, 0) AS NoTRIGGER, IFNULL(NoFUNC, 0) AS NoFUNC, IFNULL(NoPROC, 0) AS NoPROC, IFNULL(NoSched,0) AS NoSched FROM information_schema.schemata AS tbl_schema  LEFT JOIN (SELECT TABLE_SCHEMA, COUNT(*) AS NoTABLE     FROM information_schema.tables     WHERE TABLE_SCHEMA = '" + schemaName + "'    AND TABLE_TYPE='BASE TABLE'     GROUP BY TABLE_SCHEMA) AS tbl_tables ON (tbl_schema.SCHEMA_NAME=tbl_tables.TABLE_SCHEMA)  LEFT JOIN (SELECT TABLE_SCHEMA, COUNT(*) AS NoVIEW     FROM information_schema.views      WHERE TABLE_SCHEMA = '" + schemaName + "'    GROUP BY TABLE_SCHEMA) AS tbl_views ON (tbl_schema.SCHEMA_NAME=tbl_views.TABLE_SCHEMA)  LEFT JOIN (SELECT TRIGGER_SCHEMA, COUNT(*) AS NoTRIGGER     FROM information_schema.triggers     WHERE TRIGGER_SCHEMA = '" + schemaName + "'    GROUP BY TRIGGER_SCHEMA) AS tbl_trigger ON (tbl_schema.SCHEMA_NAME=tbl_trigger.TRIGGER_SCHEMA)  LEFT JOIN (SELECT ROUTINE_SCHEMA, COUNT(*) AS NoFUNC     FROM information_schema.routines     WHERE ROUTINE_TYPE = 'FUNCTION'     AND ROUTINE_SCHEMA = '" + schemaName + "'    GROUP BY ROUTINE_SCHEMA) AS tbl_function ON (tbl_schema.SCHEMA_NAME=tbl_function.ROUTINE_SCHEMA)  LEFT JOIN (SELECT ROUTINE_SCHEMA, COUNT(*) AS NoPROC     FROM information_schema.routines     WHERE ROUTINE_TYPE = 'PROCEDURE'     AND ROUTINE_SCHEMA = '" + schemaName + "'    GROUP BY ROUTINE_SCHEMA) AS tbl_procedure ON (tbl_schema.SCHEMA_NAME=tbl_procedure.ROUTINE_SCHEMA)  LEFT JOIN (SELECT EVENT_SCHEMA, COUNT(*) AS NoSched     FROM information_schema.events     WHERE EVENT_SCHEMA = '" + schemaName + "'    GROUP BY EVENT_SCHEMA) AS tbl_scheduler ON (tbl_schema.SCHEMA_NAME=tbl_scheduler.EVENT_SCHEMA)  WHERE SCHEMA_NAME = '" + schemaName + "'ORDER BY SCHEMA_NAME";
                ResultSet resultSet = statement.executeQuery(queryCountJob);
                while (resultSet.next()) {
                    String numberTable = resultSet.getString("NoTABLE");
                    String numberView = resultSet.getString("NoVIEW");
                    String numberTrigger = resultSet.getString("NoTRIGGER");
                    String numberFunction = resultSet.getString("NoFUNC");
                    String numberProcedure = resultSet.getString("NoPROC");
                    String numberSchedule = resultSet.getString("NoSched");
                    countJob.put("SCHEMA", schemaName.toUpperCase());
                    countJob.put("TABLE", numberTable);
                    countJob.put("VIEW", numberView);
                    countJob.put("TRIGGER", numberTrigger);
                    countJob.put("FUNCTION", numberFunction);
                    countJob.put("PROCEDURE", numberProcedure);
                    countJob.put("SCHEDULE", numberSchedule);
                }
                break;

            case "oracle":
                String queryCountJobOracle = "select distinct owner,object_type, count(*) cnt from dba_objects where object_type in ('TABLE','VIEW','TRIGGER','FUNCTION','PROCEDURE','SCHEDULE') and OWNER = '" + schemaName.toUpperCase() + "' group by  object_type,owner order by owner";
                ResultSet resultSet1 = statement.executeQuery(queryCountJobOracle);
                countJob.put("SCHEMA", schemaName.toUpperCase());
                while (resultSet1.next()) {
                    String objectType = resultSet1.getString("OBJECT_TYPE");
                    String count = resultSet1.getString("CNT");
                    countJob.put(objectType, count);
                }
        }
        return countJob;
    }

    public ArrayList<String> findPk(Statement statement, String tableName, String tableSchema) throws SQLException {
        ArrayList pK = new ArrayList();

        String queryKeyOracle = "SELECT cols.table_name, cols.column_name, cols.position, cons.status, cons.owner FROM all_constraints cons, all_cons_columns cols WHERE cols.table_name = ('" + tableName + "') AND cons.owner in ('" + tableSchema + "') AND cons.constraint_type in ('P','U') AND cons.constraint_name = cols.constraint_name AND cons.owner = cols.owner ORDER BY cols.table_name";

        ResultSet resultSetKey = statement.executeQuery(queryKeyOracle);
        try {
            while (resultSetKey.next()) {
                pK.add(resultSetKey.getString("COLUMN_NAME"));
            }
            return pK;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return pK;
    }

    public void getAllSchema(DBObject object) throws FileNotFoundException {
        String prepareQuery = "select username as schema_name from sys.all_users order by username";
        PrintWriter writer = new PrintWriter("schema_list.txt");
        Statement statement = databaseService.connectToDatabase(object);

        try {
            ResultSet resultSet = statement.executeQuery(prepareQuery);
            while (resultSet.next()) {
                writer.println(resultSet.getString("SCHEMA_NAME"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            writer.close();
        }

        try {
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }


    }
}
