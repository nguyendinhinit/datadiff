package org.example;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.Locale;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ReadSchemaOracle {
    static String oracleConnectionString;
    static String oracleUsername;
    static String oraclePassword;
    static String[] tableSchemas;
    static ArrayList<String> tableNames = new ArrayList<>();

    static Logger log4j = Logger.getLogger(ReadSchemaOracle.class.getName());

    public static void main(String[] args) throws IOException, SQLException {
        String filePath = "application.properties";

        try {
            Properties properties = readPropertiesFile(filePath);
            oracleConnectionString = properties.getProperty("oracle_connection_string");
            oracleUsername = properties.getProperty("oracle_username");
            oraclePassword = properties.getProperty("oracle_password");
            tableSchemas = properties.getProperty("table_schema").split(",\\s*");
        } catch (IOException e) {
            e.printStackTrace();
        }

        Statement stmt = createConnection();
        for (String tableSchema : tableSchemas) {
            log4j.info("Get list table in : " + tableSchema);
            String query = "select TABLE_NAME from SYS.ALL_TABLES where OWNER = '" + tableSchema.toUpperCase() + "'";
            ResultSet rs = stmt.executeQuery(query);
            log4j.info("Executed query: " + query);

            PrintWriter writer2 = new PrintWriter(new FileWriter("table_list.txt"));
            while (rs.next()) {
                String tableName = rs.getString("TABLE_NAME");
                tableNames.add(tableName);
                writer2.println(tableName);
            }
            writer2.close();
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
            Class.forName("oracle.jdbc.driver.OracleDriver");
            Connection conn = DriverManager.getConnection(oracleConnectionString, oracleUsername, oraclePassword);
            return conn.createStatement();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void getTableSchema(Statement stmt, String tableName) throws SQLException {
        String query = "SELECT * FROM " + tableName.toUpperCase(Locale.ROOT) + " WHERE 1=0";
        String file = "oracle_table_" + tableName + ".json";

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
//      select  owner,TABLE_NAME,COLUMN_NAME, DATA_TYPE, DATA_LENGTH from ALL_TAB_COLS where   TABLE_NAME = 'PMT_TXN' and OWNER='PAYMENT_ORDER';
        String query = "SELECT COLUMN_NAME,DATA_TYPE,DATA_PRECISION,DATA_SCALE,NULLABLE,DATA_LENGTH,DATA_DEFAULT FROM ALL_TAB_COLS where TABLE_NAME = '" + tableName.toUpperCase() + "' and OWNER='" + tableSchema.toUpperCase() + "'";
        log4j.info("Created query: " + query);

        String file = "oracle_table_" + tableName + "_v2.json";
        log4j.info("Created file: " + file);

        JSONObject tableInfo = new JSONObject(); // JSON object to hold table information
        JSONArray columnsArray = new JSONArray(); // JSON array to hold column information

        ResultSet rs = stmt.executeQuery(query);
        log4j.info("Executed query: " + query);
        while (rs.next()) {
            JSONObject columnInfo = new JSONObject();

            String columnName = rs.getString("COLUMN_NAME");
            String columnDataType = rs.getString("DATA_TYPE");
            String columnNumericPrecision = rs.getString("DATA_PRECISION");
            String columnNumericScale = rs.getString("DATA_SCALE");
            String columnIsNullable = rs.getString("NULLABLE");
            String columnCharacterMaxLenght = rs.getString("DATA_LENGTH");
            String columnType = rs.getString("DATA_TYPE");
            String columnDefault = rs.getString("DATA_DEFAULT");
            // Populate JSON object with column information

            if (columnName != null) {
                columnInfo.put("Column Name", columnName.trim());
            } else {
                columnInfo.put("Column Name", JSONObject.NULL);
            }
            if (columnDataType != null) {
                columnInfo.put("Data Type", columnDataType.trim());
            } else {
                columnInfo.put("Data Type", JSONObject.NULL);
            }
            if (columnNumericPrecision != null) {
                columnInfo.put("Data Precision", columnNumericPrecision.trim());
            } else {
                columnInfo.put("Data Precision", JSONObject.NULL);
            }
            if (columnNumericScale != null) {
                columnInfo.put("Data Scale", columnNumericScale.trim());
            } else {
                columnInfo.put("Data Scale", JSONObject.NULL);
            }
            if (columnIsNullable != null) {
                columnInfo.put("Is Nullable", columnIsNullable.trim());
            } else {
                columnInfo.put("Is Nullable", JSONObject.NULL);
            }
            if (columnCharacterMaxLenght != null) {
                columnInfo.put("Data Length", columnCharacterMaxLenght.trim());
            } else {
                columnInfo.put("Data Length", JSONObject.NULL);
            }
            if (columnType != null) {
                columnInfo.put("Column Type", columnType.trim());
            } else {
                columnInfo.put("Column Type", JSONObject.NULL);
            }
            if (columnDefault != null) {
                columnInfo.put("Column Default", columnDefault.trim());
            } else {
                columnInfo.put("Column Default", JSONObject.NULL);
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
