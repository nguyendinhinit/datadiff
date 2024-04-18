package vn.bnh.datadiff.services.impl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import vn.bnh.datadiff.dto.DBObject;
import vn.bnh.datadiff.services.QueryService;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;

public class QueryServiceImpl implements QueryService {
    Logger log4j = LogManager.getLogger(QueryServiceImpl.class);

    @Override
    public ArrayList<String> getSchema(DBObject dbObject) {
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
    public ArrayList<String> getSchema(DBObject dbObject, String query) {
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
    public ArrayList<String> getTable(DBObject dbObject, String schema) {
        ArrayList<String> tableList = new ArrayList<>();
        String dbName = dbObject.getDbname();
        try {
            Statement statement = getStatement(dbObject);
            switch (dbName) {
                case "oracle":
                    String query = String.format("SELECT table_name FROM all_tables WHERE owner = '%s'", schema);
                    ResultSet rs = statement.executeQuery(query);
                    while (rs.next()){
                        tableList.add(rs.getString("TABLE_NAME"));
                    }
                int tableCount = tableList.size();
                    log4j.info("Load {} table from {} schema",tableCount,schema);
            }
        } catch (Exception e) {
            e.printStackTrace();
            log4j.error("Can not query table from {}", schema);
        }
        return tableList;
    }
}