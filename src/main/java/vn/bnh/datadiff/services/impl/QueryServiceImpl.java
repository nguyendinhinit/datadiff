package vn.bnh.datadiff.services.impl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import vn.bnh.datadiff.dto.DBObject;
import vn.bnh.datadiff.dto.TableObject;
import vn.bnh.datadiff.services.QueryService;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Stream;

public class QueryServiceImpl implements QueryService {
    Logger log4j = LogManager.getLogger(QueryServiceImpl.class);

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
            String dbName = dbObject.getDbname();
            try {
                ArrayList<String> schemaList = new ArrayList<>();
                Statement stmt = getStatement(dbObject);
                switch (dbName) {
                    case "oracle":
                        String queryOracle = String.format("SELECT username from dba_users where username not in (SELECT distinct(OWNER) FROM sys.dba_tab_privs WHERE grantee='PUBLIC')");
                        ResultSet rs = stmt.executeQuery(queryOracle);
                        while (rs.next()) {
                            schemaList.add(rs.getString("username"));
                        }
                        int schemaCount = schemaList.size();
                        log4j.info("Loading {} schema from {}", schemaCount, dbName);
                        return schemaList;
                    case "mysql":
                        String queryMysql = String.format("SELECT schema_name FROM information_schema.schemata");
                        ResultSet rsMysql = stmt.executeQuery(queryMysql);
                        while (rsMysql.next()) {
                            schemaList.add(rsMysql.getString("schema_name"));
                        }
                        int schemaCountMysql = schemaList.size();
                        log4j.info("Loading {} schema from {}", schemaCountMysql, dbName);
                        return schemaList;
                }
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
        String query = null;
        try {
            Statement statement = getStatement(dbObject);
            switch (dbName) {
                case "oracle":
                    query = String.format("SELECT table_name FROM all_tables WHERE owner = '%s'", schema);
                    break;
                case "mysql":
                    query = String.format("SELECT table_name FROM information_schema.tables WHERE table_schema = '%s'", schema);
                    break;
            }

            ResultSet rs = statement.executeQuery(query);
            while (rs.next()) {
                tableList.add(rs.getString("TABLE_NAME").toUpperCase());
            }
            int tableCount = tableList.size();
            log4j.info("Load {} table from {} schema", tableCount, schema);
        } catch (Exception e) {
            e.printStackTrace();
            log4j.error("Can not query table from {}", schema);
        }
        return tableList;
    }

    @Override
    public ArrayList<TableObject> getTableMetadata(DBObject dbObject, String tableName, String schemaName) {
        ArrayList<TableObject> tableMetadata = new ArrayList<>();
        String dbName = dbObject.getDbname();
        switch (dbName) {
            case "oracle":
                String query = String.format("SELECT COLUMN_NAME, DATA_TYPE,DATA_LENGTH, DATA_PRECISION, DATA_SCALE, NULLABLE, DATA_DEFAULT FROM all_tab_columns where TABLE_NAME = '%s' and OWNER='%s' order by COLUMN_NAME", tableName, schemaName);
                log4j.info("Excecute query {}", query);
                try {
                    Statement statement = getStatement(dbObject);
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

    public ArrayList<TableObject> getTableMetadata(DBObject dbObject, String tableName, String schemaName, ArrayList<String> pKs) {
        ArrayList<TableObject> tableMetadata = new ArrayList<>();
        String dbName = dbObject.getDbname();
        String query = null;
        log4j.info("Found primary key {}", pKs.toString());

        switch (dbName) {
            case "oracle":
                query = String.format("SELECT COLUMN_NAME as CL, DATA_TYPE as DT,DATA_LENGTH AS DL, DATA_PRECISION AS DP, DATA_SCALE as DS, NULLABLE as DN, DATA_DEFAULT as DD, NULL as DDP FROM all_tab_columns where TABLE_NAME = '%s' and OWNER='%s' order by COLUMN_NAME", tableName, schemaName);
                log4j.info("Execute query {}", query);

                break;
            case "mysql":
                query = String.format("SELECT COLUMN_NAME as CL, IS_NULLABLE as DN,DATA_TYPE as DT,CHARACTER_MAXIMUM_LENGTH as DL,NUMERIC_PRECISION as DP,NUMERIC_SCALE as DS,COLUMN_DEFAULT as DD, DATETIME_PRECISION as DDP FROM information_schema.columns WHERE TABLE_SCHEMA = '%s' and TABLE_NAME='%s'", schemaName, tableName);
                log4j.info("Execute query {}", query);
                break;
        }

        try {
            Statement statement = getStatement(dbObject);
            ResultSet rs = statement.executeQuery(query);
            while (rs.next()) {
                Stream<String> pkStream = pKs.stream();
                TableObject tableObject = new TableObject();
                tableObject.setColumnName(rs.getString("CL"));
                tableObject.setDataType(rs.getString("DT"));
                tableObject.setDataLength(rs.getString("DL"));
                tableObject.setDataPrecision(rs.getString("DP"));
                tableObject.setDataScale(rs.getString("DS"));
                tableObject.setNullable(rs.getString("DN"));
                tableObject.setDataDefault(rs.getString("DD"));
                tableObject.setDateTimePrecision(rs.getString("DDP"));

                if (pkStream.anyMatch(rs.getString("CL")::equals)) {
                    tableObject.setPKs("PRI");
                }

                tableMetadata.add(tableObject);
                log4j.info("loaded column {} metadata with data type {}, and data length {}, data precision {}, data scale {}, data nullable {}, data default {}, data pk {}", tableObject.getColumnName(), tableObject.getDataType(), tableObject.getDataLength(), tableObject.getDataPrecision(), tableObject.getDataScale(), tableObject.getNullable(), tableObject.getDataDefault(), tableObject.getPKs());
            }
            int columnCount = tableMetadata.size();
            log4j.info("Load {} column from {} table", columnCount, tableName);
//                statement.close();
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

    public LinkedHashMap<String, Map<String, ArrayList<TableObject>>> getDbMetadata(DBObject dbObject) {
        String dbName = dbObject.getDbname();
        log4j.info("Start loading {} metadata", dbName);
        LinkedHashMap<String, Map<String, ArrayList<TableObject>>> dbMetadata = new LinkedHashMap<>();

        ArrayList<String> schemaList = getSchemaList(dbObject);
        int progress = 1;
        for (String schema : schemaList) {
            ArrayList<String> tableList = getTableList(dbObject, schema);
            Map<String, ArrayList<TableObject>> tblMds = new HashMap<>();
            for (String table : tableList) {
                log4j.info("Start query {} table at schema {}", table, schema);
                ArrayList<String> pKs = findPk(dbObject, table, schema);
                ArrayList<String> incremental = findIncremental(dbObject, table, schema);
//                ArrayList<String>
                ArrayList<TableObject> tblMd = getTableMetadata(dbObject, table, schema, pKs);
                tblMds.put(table, tblMd);
                log4j.info("Finish query metadata of {} table at schema {}", table, schema);
            }
            double percent = (double) (progress * 100 / schemaList.size());
            log4j.info("Process {}%", percent);
            progress++;
            dbMetadata.put(schema, tblMds);
        }
        return dbMetadata;
    }

    public ArrayList<String> findPk(DBObject dbObject, String tableName, String tableSchema) {
        Statement stmt = getStatement(dbObject);
        String dbName = dbObject.getDbname();
        ArrayList<String> pK = new ArrayList<>();
        String queryKey = null;
        switch (dbName) {
            case "oracle":
                queryKey = String.format("SELECT cols.table_name, cols.column_name as PK, cols.position, cons.status, cons.owner FROM all_constraints cons, all_cons_columns cols WHERE cols.table_name = ('%s') AND cons.owner in ('%s') AND cons.constraint_type in ('P','U') AND cons.constraint_name = cols.constraint_name AND cons.owner = cols.owner ORDER BY cols.table_name", tableName, tableSchema);
                break;
            case "mysql":
                queryKey = String.format("SELECT k.column_name as PK FROM information_schema.table_constraints t JOIN information_schema.key_column_usage k USING(constraint_name,table_schema,table_name) WHERE t.constraint_type='PRIMARY KEY'   AND t.table_schema='%s'   AND t.table_name='%s'", tableSchema, tableName);
        }
        try {
            ResultSet resultSetKey = stmt.executeQuery(queryKey);
            log4j.info("Execute query {}", queryKey);
            log4j.info("Start retrieve primary key for table {}, at schema {}, db {}", tableName, tableSchema, dbName);
            while (resultSetKey.next()) {
                pK.add(resultSetKey.getString("PK"));
            }
            return pK;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return pK;
    }

    ArrayList<String> findIncremental(DBObject dbObject, String tableName, String schemaName){
        Statement statement = getStatement(dbObject);
        String query = null;
        String dbName = dbObject.getDbname();
        switch (dbName) {
            case "oracle":
                query = String.format("SELECT COLUMN_NAME FROM all_tab_columns WHERE IDENTITY_COLUMN = 'YES' and OWNER = '%s' and TABLE_NAME = '%s'",schemaName, tableName);
                break;
            case "mysql":
                query = String.format("SELECT COLUMN_NAME FROM information_schema.columns WHERE EXTRA = 'auto_increment' and TABLE_SCHEMA = '%s' and TABLE_NAME = '%s'", schemaName, tableName);
                break;
        }
        try {
            ResultSet rs = statement.executeQuery(query);
            ArrayList<String> incremental = new ArrayList<>();
            while (rs.next()) {
                incremental.add(rs.getString("COLUMN_NAME"));
            }
            return incremental;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}