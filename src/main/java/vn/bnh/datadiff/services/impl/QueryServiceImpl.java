package vn.bnh.datadiff.services.impl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import vn.bnh.datadiff.dto.DBObject;
import vn.bnh.datadiff.dto.TableObject;
import vn.bnh.datadiff.services.QueryService;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class QueryServiceImpl implements QueryService {
    Logger log4j = LogManager.getLogger(QueryServiceImpl.class);

    @Override
    public ArrayList<String> getSchemaList(DBObject dbObject) {
        log4j.info("Query properties not found, starting get schema list from source database with default query");
        log4j.info("Starting get schema list from source database");
        String dbName = dbObject.getDbname();
        String connectionString = dbObject.getConnectionString();
        String username = dbObject.getUsername();
        String password = dbObject.getPassword();
        try {
            ArrayList<String> schemaList = new ArrayList<>();
            Statement stmt = getStatement(dbObject);
            switch (dbName) {
                case "oracle":
                    String query = "SELECT username from dba_users where username not in (SELECT distinct(OWNER) FROM sys.dba_tab_privs WHERE grantee='PUBLIC')";
                    ResultSet rs = stmt.executeQuery(query);
                    while (rs.next()) {
                        schemaList.add(rs.getString("username"));
                    }
                    int schemaCount = schemaList.size();
                    log4j.info("Loading {} schema from {}", schemaCount, dbName);
                    return schemaList;
            }
        } catch (Exception e) {
            e.printStackTrace();
            log4j.error("Connection Error please check {}", connectionString);
        }
        return null;
    }

    @Override
    public ArrayList<String> getSchemaList(DBObject dbObject, String query) {
        String dbName = dbObject.getDbname();
        String connectionString = dbObject.getConnectionString();
        String username = dbObject.getUsername();
        String password = dbObject.getPassword();
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

    public Connection getConnection(String dbURL, String userName,
                                    String password, String dbName) {
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
        try {
            Connection connection = getConnection(connectionString, userName, password, dbName);
            Statement statement = connection.createStatement();
            return statement;
        } catch (Exception e) {
            e.printStackTrace();
            log4j.error("Can not create statement to {}", dbName);
        }
        return null;
    }

    @Override
    public ArrayList<String> getTableList(DBObject dbObject, String schema) {
        ArrayList<String> tableList = new ArrayList<>();
        String dbName = dbObject.getDbname();
        try {
            Statement statement = getStatement(dbObject);
            switch (dbName) {
                case "oracle":
                    String query = String.format("SELECT table_name FROM all_tables WHERE owner = '%s'", schema);
                    ResultSet rs = statement.executeQuery(query);
                    while (rs.next()) {
                        tableList.add(rs.getString("TABLE_NAME"));
                    }
                    int tableCount = tableList.size();
                    log4j.info("Load {} table from {} schema", tableCount, schema);
            }
        } catch (Exception e) {
            e.printStackTrace();
            log4j.error("Can not query table from {}", schema);
        }
        return tableList;
    }

    @Override
    public ArrayList<TableObject> getTableMetadata(DBObject dbOject, String tableName, String schemaName) {
        ArrayList<TableObject> tableMetadata = new ArrayList<>();
        String dbName = dbOject.getDbname();
        switch (dbName) {
            case "oracle":
                String query = String.format("SELECT COLUMN_NAME, DATA_TYPE,DATA_LENGTH, DATA_PRECISION, DATA_SCALE, NULLABLE, DATA_DEFAULT FROM all_tab_columns where TABLE_NAME = '%s' and OWNER='%s' order by COLUMN_NAME", tableName, schemaName);
                log4j.info("Excecute query {}", query);
                try {
                    Statement statement = getStatement(dbOject);
                    ResultSet rs = statement.executeQuery(query);
                    while (rs.next()) {
                    }
                    int columnCount = tableMetadata.size();
                    log4j.info("Load {} column from {} table", columnCount, tableName);
                } catch (Exception e) {
                    e.printStackTrace();
                    log4j.error("Can not query table metadata from {}", tableName);
                }
        }
        return tableMetadata;
    }

    public ArrayList<TableObject> getTableMetadata(DBObject dbOject, String tableName, String schemaName, ArrayList<String> pKs) {
        ArrayList<TableObject> tableMetadata = new ArrayList<>();
        String dbName = dbOject.getDbname();
        switch (dbName) {
            case "oracle":
                String query = String.format("SELECT COLUMN_NAME, DATA_TYPE,DATA_LENGTH, DATA_PRECISION, DATA_SCALE, NULLABLE, DATA_DEFAULT FROM all_tab_columns where TABLE_NAME = '%s' and OWNER='%s' order by COLUMN_NAME", tableName, schemaName);
                log4j.info("Excecute query {}", query);
                try {
                    Statement statement = getStatement(dbOject);
                    ResultSet rs = statement.executeQuery(query);
                    while (rs.next()) {
                    }
                    int columnCount = tableMetadata.size();
                    log4j.info("Load {} column from {} table", columnCount, tableName);
                } catch (Exception e) {
                    e.printStackTrace();
                    log4j.error("Can not query table metadata from {}", tableName);
                }
        }
        return tableMetadata;
    }


    //todo: Release later
    @Override
    public LinkedHashMap<String, Map<String, ArrayList<String>>> getDbMetadata(DBObject dbObject, ArrayList<String> schemaList) {
        LinkedHashMap<String, Map<String, ArrayList<String>>> dbMetadata = new LinkedHashMap<>();
        return dbMetadata;
    }

    public LinkedHashMap<String, Map<String, ArrayList<TableObject>>> getDbMetadata(DBObject dbObject) {
        String dbName = dbObject.getDbname();
        switch (dbName) {
            case "oracle":
                log4j.info("Start loading {} metadata", dbName);
                LinkedHashMap<String, Map<String, ArrayList<TableObject>>> dbMetadata = new LinkedHashMap<>();

                //todo: if else with custom query
                ArrayList<String> schemaList = getSchemaList(dbObject);

                int progess = 1;
                for (String schema : schemaList) {
                    ArrayList<String> tableList = getTableList(dbObject, schema);
                    Map<String, ArrayList<TableObject>> tableMetadatas = new HashMap<>();
                    for (String table : tableList) {
                        log4j.info("Start compare {} table at schema {}", table, schema);
                        ArrayList<String> pKs = findPk(dbObject, table, schema);
                        ArrayList<TableObject> tblMd = getTableMetadata(dbObject, table, schema);
                        tableMetadatas.put(table, tblMd);
                    }
                    int percent = progess / schemaList.size();
                    log4j.info("Progess {}%", percent);
                    progess++;
                    dbMetadata.put(schema, tableMetadatas);
                }
                return dbMetadata;
        }
    return null;
    }

    public ArrayList<String> findPk(DBObject dbObject, String tableName, String tableSchema) {
        Statement stmt = getStatement(dbObject);
        ArrayList pK = new ArrayList();
        String queryKeyOracle = String.format("SELECT cols.table_name, cols.column_name, cols.position, cons.status, cons.owner FROM all_constraints cons, all_cons_columns cols WHERE cols.table_name = ('%s') AND cons.owner in ('%s') AND cons.constraint_type in ('P','U') AND cons.constraint_name = cols.constraint_name AND cons.owner = cols.owner ORDER BY cols.table_name", tableName, tableSchema);
        try {
            ResultSet resultSetKey = stmt.executeQuery(queryKeyOracle);
            while (resultSetKey.next()) {
                pK.add(resultSetKey.getString("COLUMN_NAME"));
            }
            return pK;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return pK;
    }
}