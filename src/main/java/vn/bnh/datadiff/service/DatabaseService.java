package vn.bnh.datadiff.service;

import vn.bnh.datadiff.dto.MysqlObject;
import vn.bnh.datadiff.dto.OracleObject;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Logger;

public class DatabaseService {
    Logger log4j = Logger.getLogger(DatabaseService.class.getName());

    public Statement connectMysql(MysqlObject mysqlObject) {
        log4j.info("Connecting to MySQL database with connection string: " + mysqlObject.getMysqlConnection() + " with user: " + mysqlObject.getMysqlUser() + " and password: " + mysqlObject.getMysqlPassword());

        try {
            Statement statement = createConnection(mysqlObject.getMysqlConnection(), mysqlObject.getMysqlUser(), mysqlObject.getMysqlPassword());
            return statement;
        } catch (SQLException e) {
            e.printStackTrace();
        }

        log4j.info("Connected to MySQL database successfully");
        return null;
    }

    public Statement connectOracle(OracleObject oracleObject) {
        log4j.info("Connecting to Oracle database with connection string: " + oracleObject.getOracleConnection() + " with user: " + oracleObject.getOracleUser() + " and password: " + oracleObject.getOraclePassword());

        try {
            Statement statement = createConnection(oracleObject.getOracleConnection(), oracleObject.getOracleUser(), oracleObject.getOraclePassword());
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
