package vn.bnh.datadiff.service;

import vn.bnh.datadiff.dto.DBObject;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Logger;

public class DatabaseService {
    Logger log4j = Logger.getLogger(DatabaseService.class.getName());

    public Statement connectToDatabase(DBObject dbObject) {
        log4j.info("Connecting to "+dbObject.getDatabase()+" database with connection string: " + dbObject.getConnectionString() + " with user: " + dbObject.getUserName() + " and password: " + dbObject.getPassword());

        try {
            Statement statement = createConnection(dbObject.getConnectionString(), dbObject.getUserName(), dbObject.getPassword());
            return statement;
        } catch (SQLException e) {
            e.printStackTrace();
        }

        log4j.info("Connected to MySQL database successfully");
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
