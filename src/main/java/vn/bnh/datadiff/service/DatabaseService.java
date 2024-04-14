package vn.bnh.datadiff.service;

import vn.bnh.datadiff.dto.DBObject;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Logger;

public class DatabaseService {
    Logger log4j = Logger.getLogger(DatabaseService.class.getName());

    public Statement connectMysql(DBObject mysqlObject) {
        log4j.info("Connecting to MySQL database with connection string: " + mysqlObject.getConnectionString() + " with user: " + mysqlObject.getUserName() + " and password: " + mysqlObject.getPassword());

        try {
            Statement statement = createConnection(mysqlObject.getConnectionString(), mysqlObject.getUserName(), mysqlObject.getPassword());
            return statement;
        } catch (SQLException e) {
            e.printStackTrace();
        }

        log4j.info("Connected to MySQL database successfully");
        return null;
    }

    public Statement connectOracle(DBObject oracleObject) {
        log4j.info("Connecting to Oracle database with connection string: " + oracleObject.getConnectionString() + " with user: " + oracleObject.getUserName() + " and password: " + oracleObject.getPassword());

        try {
            Statement statement = createConnection(oracleObject.getConnectionString(), oracleObject.getUserName(), oracleObject.getPassword());
            return statement;
        } catch (SQLException e) {
            e.printStackTrace();
        }

        log4j.info("Connected to Oracle database successfully");

        return null;
    }

    public Statement createConnection(String connection, String username, String password) throws SQLException {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            Connection conn = DriverManager.getConnection(connection, username, password);
            return conn.createStatement();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }
}
