package org.example;

import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.JSONArray;
import org.json.JSONObject;

public class ReadSchemaMysql {
    static String mysqlConnectionString;
    static String mysqlUsername;
    static String mysqlPassword;
    static String[] tableSchemas;
    static ArrayList<String> tableNames = new ArrayList<>();
    static Logger log4j = Logger.getLogger(ReadSchemaMysql.class.getName());

    public static void main(String[] args) throws SQLException, IOException {
        String filePath = "application.properties";

        try {
            Properties properties = readPropertiesFile(filePath);
            mysqlConnectionString = properties.getProperty("mysql_connection_string");
            mysqlUsername = properties.getProperty("mysql_username");
            mysqlPassword = properties.getProperty("mysql_password");
            tableSchemas = properties.getProperty("table_schema").split(",\\s*");
        } catch (IOException e) {
            e.printStackTrace();
        }

        Statement stmt = createConnection();
        for (String tableSchema : tableSchemas) {
            log4j.info("Get list table in : " + tableSchema);
            ResultSet rs = stmt.executeQuery("select TABLE_NAME from information_schema.TABLES where table_schema = '" + tableSchema+"'");
            while (rs.next()) {
                String tableName = rs.getString("TABLE_NAME");
                log4j.info("Table: " + tableName);
                tableNames.add(tableName);
            }
            for (String tableName : tableNames) {
                log4j.info("Table: " + tableName);
                getTableSchemav2(stmt, tableName, tableSchema);
            }
        }
        stmt.close();
    }

    private static Properties readPropertiesFile(String fileName) throws IOException {
        Properties properties = new Properties();
        try (InputStream inputStream = Main.class.getClassLoader().getResourceAsStream(fileName)) {
            properties.load(inputStream);
        }
        return properties;
    }

    public static Statement createConnection() throws SQLException {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            Connection conn = DriverManager.getConnection(mysqlConnectionString, mysqlUsername, mysqlPassword);
            return conn.createStatement();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
            return null;
    }

    public static void getTableSchema(Statement stmt, String tableName) throws SQLException {
        String query = "SELECT * FROM " + tableName + " LIMIT 0";
        String file = "mysql_table_" + tableName + ".json";

        JSONObject tableInfo = new JSONObject(); // JSON object to hold table information
        JSONArray columnsArray = new JSONArray(); // JSON array to hold column information

        ResultSet rs = stmt.executeQuery(query);

        // Get ResultSet metadata
        ResultSetMetaData metaData = rs.getMetaData();

        // Get column count
        int columnCount = metaData.getColumnCount();

        // Print column information

        for (int i = 1; i <= columnCount; i++) {
            JSONObject columnInfo = new JSONObject();
            String columnName = metaData.getColumnName(i);
            String columnType = metaData.getColumnTypeName(i);
            int columnSize = metaData.getColumnDisplaySize(i);
            boolean isNullable = (metaData.isNullable(i) == ResultSetMetaData.columnNullable);

            // Populate JSON object with column information
            columnInfo.put("name", columnName);
            columnInfo.put("type", columnType);
            columnInfo.put("size", columnSize);
            columnInfo.put("nullable", isNullable);

            columnsArray.put(columnInfo);

        }

        // Add the JSON array of columns to the table JSON object
        tableInfo.put(tableName, columnsArray);

        // Write JSON object to file
        try (PrintWriter writer = new PrintWriter(new FileWriter(file))) {
            writer.println(tableInfo.toString(4)); // Indented JSON with 4 spaces
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void getTableSchemav2(Statement stmt, String tableName, String tableSchema) throws SQLException {
//        select * from information_schema.columns where TABLE_NAME = 'pmt_txn';
        String query = "SELECT COLUMN_NAME,COLUMN_DEFAULT,IS_NULLABLE,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH,NUMERIC_PRECISION,NUMERIC_SCALE,DATETIME_PRECISION,COLUMN_TYPE FROM information_schema.columns where TABLE_NAME = '" + tableName +"' and TABLE_SCHEMA='"+ tableSchema+"'";
        log4j.info("Created query: " + query);

        String file = "mysql_table_" + tableName + "_v2.json";
        log4j.info("Created file: " + file);

        JSONObject tableInfo = new JSONObject(); // JSON object to hold table information
        JSONArray columnsArray = new JSONArray(); // JSON array to hold column information

        ResultSet rs = stmt.executeQuery(query);
        log4j.info("Executed query: " + query);
        while(rs.next()) {
            JSONObject columnInfo = new JSONObject();
            String columnName = rs.getString("COLUMN_NAME");
            String columnDefault = rs.getString("COLUMN_DEFAULT");
            String columnIsNullable = rs.getString("IS_NULLABLE");
            String columnDataType = rs.getString("DATA_TYPE");
            String columnCharacterMaxLenght = rs.getString("CHARACTER_MAXIMUM_LENGTH");
            String columnNumericPrecision = rs.getString("NUMERIC_PRECISION");
            String columnNumericScale = rs.getString("NUMERIC_SCALE");
            String columnDateTimePrecision = rs.getString("DATETIME_PRECISION");
            String columnType = rs.getString("COLUMN_TYPE");
            if (columnName != null) {
                columnInfo.put("Column Name", columnName.trim());
            } else {
                columnInfo.put("Column Name", JSONObject.NULL);
            }
            if (columnDefault != null) {
                columnInfo.put("Column Default", columnDefault.trim());
            } else {
                columnInfo.put("Column Default", JSONObject.NULL);
            }
            if (columnIsNullable != null) {
                columnInfo.put("Is Nullable", columnIsNullable.trim());
            } else {
                columnInfo.put("Is Nullable", JSONObject.NULL);
            }
            if (columnDataType != null) {
                columnInfo.put("Data Type", columnDataType.trim());
            } else {
                columnInfo.put("Data Type", JSONObject.NULL);
            }
            if (columnCharacterMaxLenght != null) {
                columnInfo.put("Character Max Length", columnCharacterMaxLenght.trim());
            } else {
                columnInfo.put("Character Max Length", JSONObject.NULL);
            }
            if (columnNumericPrecision != null) {
                columnInfo.put("Numeric Precision", columnNumericPrecision.trim());
            } else {
                columnInfo.put("Numeric Precision", JSONObject.NULL);
            }
            if (columnNumericScale != null) {
                columnInfo.put("Numeric Scale", columnNumericScale.trim());
            } else {
                columnInfo.put("Numeric Scale", JSONObject.NULL);
            }
            if (columnDateTimePrecision != null) {
                columnInfo.put("DateTime Precision", columnDateTimePrecision.trim());
            } else {
                columnInfo.put("DateTime Precision", JSONObject.NULL);
            }
            if (columnType != null) {
                columnInfo.put("Column Type", columnType.trim());
            } else {
                columnInfo.put("Column Type", JSONObject.NULL);
            }
            columnsArray.put(columnInfo);
        }
        tableInfo.put(tableName, columnsArray);

        // Write JSON object to file
        try (PrintWriter writer = new PrintWriter(new FileWriter(file))) {
            writer.println(tableInfo.toString(4)); // Indented JSON with 4 spaces
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
