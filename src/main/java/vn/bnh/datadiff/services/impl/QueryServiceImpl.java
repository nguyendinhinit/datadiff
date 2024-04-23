package vn.bnh.datadiff.services.impl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import vn.bnh.datadiff.dto.ColumnObject;
import vn.bnh.datadiff.dto.DBObject;
import vn.bnh.datadiff.services.QueryBuilderService;
import vn.bnh.datadiff.services.QueryService;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Stream;

public class QueryServiceImpl implements QueryService {
    QueryBuilderService queryBuilderService = new QueryBuilderServiceImpl();
    Logger log4j = LogManager.getLogger(QueryServiceImpl.class);
    Connection oracleConn;
    Connection mysqlConn;

    @Override
    public ArrayList<String> getSchemaList(DBObject dbObject) {
        File file = new File("schemas.txt");
        if (file.exists()) {
            log4j.info("schemas.txt found, starting get schema list from source database with query in file");
            try {
                Reader reader = new FileReader(file);
                BufferedReader bufferedReader = new BufferedReader(reader);
                ArrayList<String> schemaList = new ArrayList<>();
                String line;
                while ((line = bufferedReader.readLine()) != null) {
                    schemaList.add(line);
                }
                int schemaCount = schemaList.size();
                log4j.info("Loading {} schema from file", schemaCount);
                return schemaList;
            } catch (Exception e) {
                e.printStackTrace();
                log4j.error("Can not read schemas.txt");
            }
        } else {
            log4j.info("Query properties not found, starting get schema list from source database with default query");
            log4j.info("Starting get schema list from source database");
            String queryBuilder = queryBuilderService.buildQuery(dbObject, "schema");
            String dbName = dbObject.getDbname();
            String query = String.format(queryBuilder, "null");
            ResultSet rs = executeQuery(dbObject, query);
            try {
                ArrayList<String> schemaList = new ArrayList<>();
                while (rs.next()) {
                    schemaList.add(rs.getString("username"));
                }
                int schemaCount = schemaList.size();
                log4j.info("Loading {} schema from {}", schemaCount, dbName);
                return schemaList;
            } catch (Exception e) {
                e.printStackTrace();
                log4j.error("Connection Error please check metadata");
            }
        }
        return null;
    }

    @Override
    public ArrayList<String> getSchemaList(DBObject dbObject, String query) {
        String dbName = dbObject.getDbname();
        String connectionString = dbObject.getConnectionString();

        try {
            ArrayList<String> schemaList = new ArrayList<>();
            Statement stmt = getStatement(dbObject);
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {
                schemaList.add(rs.getString("username"));
            }
            int schemaCount = rs.getRow();
            log4j.info("load {} schema from source {}", schemaCount, dbName);
            return schemaList;
        } catch (Exception e) {
            e.printStackTrace();
            log4j.error("Connection Error please check {}", connectionString);
        }
        return null;
    }

    public Connection getConnection(String dbURL, String userName, String password, String dbName) {
        Connection conn = null;
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            Class.forName("oracle.jdbc.driver.OracleDriver");
            conn = DriverManager.getConnection(dbURL, userName, password);
            log4j.info("connect to {} successfully!", dbName);
        } catch (Exception ex) {
            log4j.error("connect failure!");
            ex.printStackTrace();
        }
        return conn;
    }

    public Statement getStatement(DBObject dbObject) {
        String connectionString = dbObject.getConnectionString();
        String userName = dbObject.getUsername();
        String password = dbObject.getPassword();
        String dbName = dbObject.getDbname();
        Connection connection;
        if (connectionString.startsWith("jdbc:oracle")) {
            if (oracleConn == null) {
                oracleConn = getConnection(connectionString, userName, password, dbName);
            }
            connection = oracleConn;
        } else {
            if (mysqlConn == null) {
                mysqlConn = getConnection(connectionString, userName, password, dbName);
            }
            connection = mysqlConn;
        }
        try {
            return connection.createStatement();
        } catch (Exception e) {
            log4j.error("Can not create statement to {}: {}", dbName, e);
        }
        return null;
    }

    @Override
    public ArrayList<String> getTableList(DBObject dbObject, String schema) {
        ArrayList<String> tableList = new ArrayList<>();
        String queryBuilder = queryBuilderService.buildQuery(dbObject, "table");
        String query = String.format(queryBuilder, schema);
        ResultSet rs = executeQuery(dbObject, query);
        try {
            while (rs.next()) {
                tableList.add(rs.getString("TABLE_NAME").toUpperCase());
            }
            int tableCount = tableList.size();
            log4j.info("Load {} table from {} schema", tableCount, schema);
        } catch (Exception e) {
            log4j.error("Can not query table list from {}: {}", schema, e);
        }
        return tableList;
    }

    public ArrayList<ColumnObject> getColumnMetadata(DBObject dbObject, String tableName, String schemaName, ArrayList<String> pKs, ArrayList<String> incremental) {
        ArrayList<ColumnObject> tableMetadata = new ArrayList<>();
        String query = queryBuilderService.buildQuery(dbObject, "column");
        query = String.format(query, schemaName, tableName);
        if (!pKs.isEmpty()) log4j.info("Found primary key {}", pKs.toString());
        try {
            Statement statement = getStatement(dbObject);
            ResultSet rs = statement.executeQuery(query);
            while (rs.next()) {
                Stream<String> pkStream = pKs.stream();
                Stream<String> incrementalStream = incremental.stream();
                ColumnObject columnObject = new ColumnObject();
                columnObject.setSchemaName(schemaName);
                columnObject.setTableName(tableName);
                columnObject.setColumnName(rs.getString("CL"));
                columnObject.setDataType(rs.getString("DT"));
                columnObject.setDataLength(rs.getString("DL"));
                columnObject.setDataPrecision(rs.getString("DP"));
                columnObject.setDataScale(rs.getString("DS"));
                columnObject.setNullable(String.valueOf(rs.getString("DN").charAt(0)));
                String dataScale = rs.getString("DD");
                if (dataScale != null) {
                    columnObject.setDataDefault(dataScale.trim());
                } else {
                    columnObject.setDataDefault("null");
                }
                columnObject.setDateTimePrecision(rs.getString("DDP"));
                if (columnObject.getDateTimePrecision() == null) {
                    String dataType = String.format("%s(%s)", columnObject.getDataType(), columnObject.getDataLength());
                    columnObject.setDataPrecision(dataType);
                }
                if (pkStream.anyMatch(rs.getString("CL")::equals)) {
                    columnObject.setPrimaryKey("PRI");
                }

                if (incrementalStream.anyMatch(rs.getString("CL")::equals)) {
                    columnObject.setIncremental("YES");
                }

                tableMetadata.add(columnObject);
                log4j.info("loaded column {} metadata with data type {}, and data length {}, data precision {}, data scale {}, data nullable {}, data default {}, data pk {}", columnObject.getColumnName(), columnObject.getDataType(), columnObject.getDataLength(), columnObject.getDataPrecision(), columnObject.getDataScale(), columnObject.getNullable(), columnObject.getDataDefault(), columnObject.getPrimaryKey());
            }
            int columnCount = tableMetadata.size();
            log4j.info("Load {} column from {} table", columnCount, tableName);
        } catch (Exception e) {
            e.printStackTrace();
            log4j.error("Can not query table metadata from {}", tableName);
        }
        return tableMetadata;
    }


    //todo: Release later
    @Override
    public LinkedHashMap<String, Map<String, ArrayList<String>>> getDbMetadata(DBObject dbObject, ArrayList<String> schemaList) {
        return new LinkedHashMap<>();
    }

    public LinkedHashMap<String, Map<String, ArrayList<ColumnObject>>> getDbMetadata(DBObject dbObject) {
        String dbName = dbObject.getDbname();
        log4j.info("Start loading {} metadata", dbName);
        LinkedHashMap<String, Map<String, ArrayList<ColumnObject>>> dbMetadata = new LinkedHashMap<>();

        ArrayList<String> schemaList = getSchemaList(dbObject);
        int progress = 1;
        for (String schema : schemaList) {
            ArrayList<String> tableList = getTableList(dbObject, schema);
            Map<String, ArrayList<ColumnObject>> tblMds = new HashMap<>();
            for (String table : tableList) {
                log4j.info("Start query {} table at schema {}", table, schema);
                ArrayList<String> primaryKeys = findPrimaryKey(dbObject, table, schema);
                ArrayList<String> incremental = findIncremental(dbObject, table, schema);
                ArrayList<ColumnObject> clMds = getColumnMetadata(dbObject, table, schema, primaryKeys, incremental);
                tblMds.put(table, clMds);
                log4j.info("Finish query metadata of {} table at schema {}", table, schema);
            }
            double percent = ((double) (progress * 100) / schemaList.size());
            log4j.info("Process {}%", percent);
            progress++;
            dbMetadata.put(schema, tblMds);
        }
        return dbMetadata;
    }

    /**
     * This function use when the query is defined in the properties file
     */
    @Override
    public LinkedHashMap<String, Map<String, ArrayList<ColumnObject>>> getDbMetadata(DBObject dbObject, String query) {
        log4j.info("Founded custom query from properties file");
        return null;
    }

    public ArrayList<String> findPrimaryKey(DBObject dbObject, String tableName, String tableSchema) {
        Statement stmt = getStatement(dbObject);
        String dbName = dbObject.getDbname();
        ArrayList<String> pK = new ArrayList<>();
        String queryBuilder = queryBuilderService.buildQuery(dbObject, "primaryKey");
        String queryKey = String.format(queryBuilder, tableSchema, tableName);
        try {
            ResultSet resultSetKey = stmt.executeQuery(queryKey);
            log4j.info("Execute query {}", queryKey);
            log4j.info("Start retrieve primary key for table {}, at schema {}, db {}", tableName, tableSchema, dbName);
            while (resultSetKey.next()) {
                pK.add(resultSetKey.getString("PK"));
            }
            return pK;
        } catch (Exception e) {
            log4j.error("Can not query primary key from {}: {}", tableName, e);
        }
        return pK;
    }

    ArrayList<String> findIncremental(DBObject dbObject, String tableName, String schemaName) {
        Statement statement = getStatement(dbObject);
        String queryBuilder = queryBuilderService.buildQuery(dbObject, "increment");
        String query = String.format(queryBuilder, schemaName, tableName);
        try {
            ResultSet rs = statement.executeQuery(query);
            ArrayList<String> incremental = new ArrayList<>();
            while (rs.next()) {
                incremental.add(rs.getString("COLUMN_NAME"));
            }
            return incremental;
        } catch (Exception e) {
            log4j.error("{}", e);
        }
        return null;
    }

    ArrayList<String> findForeignKeys(DBObject dbObject, String tableName, String schemaName) {
        String dbName = dbObject.getDbname();
        Statement statement = getStatement(dbObject);
        String query = queryBuilderService.buildQuery(dbObject, "foreignKey");

        try {
            ArrayList<String> foreignKeys = new ArrayList<>();
            ResultSet resultSet = statement.executeQuery(String.format(query, schemaName, tableName));
            while (resultSet.next()) {
                foreignKeys.add(resultSet.getString("COLUMN_NAME"));
            }
            return foreignKeys;
        } catch (Exception e) {
            log4j.error("Can not query foreign key from {}: {}", tableName, e);
        }
        return null;
    }

    ResultSet executeQuery(DBObject dbObject, String query) {
        try {
            Statement statement = getStatement(dbObject);
            return statement.executeQuery(query);
        } catch (Exception e) {
            log4j.error("Can not execute query {}: {}", query, e);
        }
        return null;
    }

    @Override
    public Map<String, Integer[]> countConstraintsAndIndexes(DBObject dbObject) {
        Map<String, Integer[]> result = new HashMap<>();

        String queryConstrains = queryBuilderService.buildQuery(dbObject, "constraints");
        String queryIndexes = queryBuilderService.buildQuery(dbObject, "indexes");

        ArrayList<String> schemas = getSchemaList(dbObject);
        Statement stmt1 = getStatement(dbObject);
        Statement stmt2 = getStatement(dbObject);
        for (String schema : schemas) {
            try {
                Integer[] constrainsAndIndexes = {0, 0};
                String qC = String.format(queryConstrains, schema);
                String qI = String.format(queryIndexes, schema);
                log4j.info("Execute query {}", qC);
                ResultSet rsConstrains = stmt1.executeQuery(qC);
                log4j.info("Execute query {}", qI);
                ResultSet rsIndexes = stmt2.executeQuery(qI);
                while (rsConstrains.next()) {
                    constrainsAndIndexes[0] = rsConstrains.getInt("CONSTRAINS");
                }
                while (rsIndexes.next()) {
                    constrainsAndIndexes[1] = rsIndexes.getInt("INDEXES");
                }
                result.put(schema, constrainsAndIndexes);
            } catch (Exception e) {
                e.printStackTrace();
                log4j.error("Can not count constrains and indexes");
            }

        }
        return result;
    }


    @Override
    public <T> T queryResult(Class<T> returnType) {
        return null;
    }


    @Override
    public Integer queryResult(Statement statement, String query, String schemaName, String tableName, String columnName) {

        query = String.format(query, schemaName, tableName);
        try {
            log4j.info("Execute query {}", query);
            ResultSet rs = statement.executeQuery(query);
            rs.next();
            return rs.getInt(columnName);
        } catch (Exception e) {
            log4j.error(e.getMessage());
        }
        return null;
    }

    @Override
    public Map<String, Map<String, ArrayList<Integer>>> getObjectMetadata(DBObject dbObject) {
        String dbName = dbObject.getDbname();
        log4j.info("Start loading {} metadata", dbName);
        Map<String, Map<String, ArrayList<Integer>>> objectMetadata = new HashMap<>();

        ArrayList<String> schemaList = getSchemaList(dbObject);
        int progress = 1;
        for (String schema : schemaList) {
            ArrayList<String> tableList = getTableList(dbObject, schema);
            Map<String, ArrayList<Integer>> objMds = new HashMap<>();
            for (String table : tableList) {
                log4j.info("Start query {} table at schema {}", table, schema);
                ArrayList<Integer> objectMds = getObjectMds(dbObject, schema, table);
                objMds.put(table, objectMds);
                log4j.info("Finish query metadata of {} table at schema {}", table, schema);
            }
            double percent = ((double) (progress * 100) / schemaList.size());
            log4j.info("Process {}%", percent);
            progress++;
            objectMetadata.put(schema, objMds);
        }
        return objectMetadata;
    }

    public ArrayList<Integer> getObjectMds(DBObject dbObject, String schema, String table) {
        ArrayList<Integer> objectMds = new ArrayList<>();
        String query = queryBuilderService.buildQuery(dbObject, "partition");
        query = String.format(query, schema, table);
        int partition = queryResult(getStatement(dbObject), query, schema, table, "P");
        query = queryBuilderService.buildQuery(dbObject, "index");
        query = String.format(query, schema, table);
        int index = queryResult(getStatement(dbObject), query, schema, table, "I");
        query = String.format(queryBuilderService.buildQuery(dbObject, "constraintCount"), schema, table);
        int constraint = queryResult(getStatement(dbObject), query, schema, table, "C");
        query = String.format(queryBuilderService.buildQuery(dbObject, "columnCount"), schema, table);
        int columnCount = queryResult(getStatement(dbObject), query, schema, table, "C");
        query = String.format(queryBuilderService.buildQuery(dbObject, "triggerCount"), schema, table);
        int triggerCount = queryResult(getStatement(dbObject), query, schema, table, "T");
        query = String.format(queryBuilderService.buildQuery(dbObject, "sequenceCount"), schema, table);
        int sequenceCount = queryResult(getStatement(dbObject), query, schema, table, "SC");

        objectMds.add(partition);
        objectMds.add(index);
        objectMds.add(constraint);
        objectMds.add(columnCount);
        objectMds.add(triggerCount);
        objectMds.add(sequenceCount);
        return objectMds;
    }


}