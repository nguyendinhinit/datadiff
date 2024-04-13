package org.example;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

public class Validator {
    static String[] tableNames;
    static Logger log4j = Logger.getLogger(Validator.class.getName());
    static JSONObject mysqlJson;
    static JSONObject oracleJson;

    public static void main(String[] args) throws IOException {
        System.out.println("" +
                " _____ _____ _____ _____ _____ _____ _____ _____ _____ _____ \n" +
                "|_____|_____|_____|_____|_____|_____|_____|_____|_____|_____|" +
                "");
        System.out.println("" +
                "            _ _     _       _             \n" +
                "__   ____ _| (_) __| | __ _| |_ ___  _ __ \n" +
                "\\ \\ / / _` | | |/ _` |/ _` | __/ _ \\| '__|\n" +
                " \\ V / (_| | | | (_| | (_| | || (_) | |   \n" +
                "  \\_/ \\__,_|_|_|\\__,_|\\__,_|\\__\\___/|_|   " +
                "");
        String filePath = "table_list.txt";
        String[] tableSchema = args;

        for (String schema : tableSchema) {
            PrintWriter writer = new PrintWriter(new FileWriter("report_" + schema + ".csv"));
            PrintWriter writer2 = new PrintWriter(new FileWriter("missing_" + schema + "_table.txt"));
            writer.println("tablename,oracle_columnname,oracle_data_type,oracle_default_value,oracle_nullable,mysql_colume_name,mysql_data_type,mysql_default_value,mysql_nullable,validate_name,validate_type,validate_default,validate_nullable,recommend_type");

            try {
                tableNames = Files.readAllLines(Paths.get(filePath)).toArray(new String[0]);
                log4j.info("Loaded table from list file " + Arrays.toString(tableNames));
            } catch (IOException e) {
                e.printStackTrace();
            }
            for (String tableName : tableNames) {
                try {
                    if (Files.exists(Paths.get("MYSQL_" + schema + "_" + tableName.toUpperCase() + ".json"))) {
                        log4j.info("File MYSQL_" + schema + "_" + tableName.toUpperCase() + ".json found");
                        mysqlJson = readJsonFile("MYSQL_" + schema + "_" + tableName.toUpperCase() + ".json");
                    } else {
                        log4j.info("File MYSQL_" + schema + "_" + tableName.toLowerCase() + ".json not found");
                        writer2.println(schema.toUpperCase() + "_" + tableName.toUpperCase());
                        continue;
                    }
                    if (Files.exists(Paths.get("ORACLE_" + schema + "_" + tableName + ".json"))) {
                        log4j.info("File ORACLE_" + schema + "_" + tableName + ".json found");
                        oracleJson = readJsonFile("ORACLE_" + schema + "_" + tableName + ".json");
                    }
                    csvReport(mysqlJson, oracleJson, tableName, writer);
                    log4j.info("Finish comparing table " + tableName);

                    //delete file after compare
                    Files.deleteIfExists(Paths.get("MYSQL_" + schema + "_" + tableName.toUpperCase() + ".json"));
                    Files.deleteIfExists(Paths.get("ORACLE_" + schema + "_" + tableName.toUpperCase() + ".json"));

                } catch (IOException | JSONException e) {
                    e.printStackTrace();
                }
            }

            writer.close();
            writer2.close();
        }

    }

    private static JSONObject readJsonFile(String filename) throws IOException {
        String content = new String(Files.readAllBytes(Paths.get(filename)));
        return new JSONObject(content);
    }

    private static void compareSchemas(JSONObject mysqlJson, JSONObject oracleJson, String tableName) throws IOException {
        PrintWriter writer = new PrintWriter(new FileWriter("report_table_" + tableName));
        writer.println("Comparison Report for table: " + tableName);
        Map<String, String[]> dataTypeMapping = DataTypeMapper.getDataTypeMapping();

        try {
            JSONArray mysqlArray = mysqlJson.getJSONArray(tableName);
            JSONArray oracleArray = oracleJson.getJSONArray(tableName);

            // Assuming both arrays are of equal length
            for (int i = 0; i < mysqlArray.length(); i++) {
                JSONObject mysqlObject = mysqlArray.getJSONObject(i);
                JSONObject oracleObject = oracleArray.getJSONObject(i);
                String mysqlType = mysqlObject.getString("type");
                String oracleType = oracleObject.getString("type");
                Integer mysqlSize = mysqlObject.getInt("size");
                Integer oracleSize = oracleObject.getInt("size");
                boolean mysqlNullable = mysqlObject.getBoolean("nullable");
                boolean oracleNullable = oracleObject.getBoolean("nullable");
                String mysqlName = mysqlObject.getString("name");
                String oracleName = oracleObject.getString("name");

                // Compare column names
                if (!mysqlName.equals(oracleName)) {
                    writer.println("Mismatch found for column: " + mysqlObject.getString("name"));
                    writer.println("MySQL name: " + mysqlName);
                    writer.println("Oracle name: " + oracleName);
                }
                // Compare column sizes
                if (!mysqlSize.equals(oracleSize)) {
                    writer.println("Mismatch found for column: " + mysqlObject.getString("name"));
                    writer.println("MySQL size: " + mysqlSize);
                    writer.println("Oracle size: " + oracleSize);
                }
                // Compare column nullability
                if (mysqlNullable != oracleNullable) {
                    writer.println("Mismatch found for column: " + mysqlObject.getString("name"));
                    writer.println("MySQL nullable: " + mysqlNullable);
                    writer.println("Oracle nullable: " + oracleNullable);
                }

                String[] oracleTypes = dataTypeMapping.get(oracleType);

                if (oracleTypes == null) {
                    writer.println("No mapping found for MySQL type: " + mysqlType);
                    continue;
                }
                if (Arrays.stream(oracleTypes).noneMatch(mysqlType::equals)) {
                    writer.println("Mismatch found for column: " + mysqlObject.getString("name"));
                    writer.println("MySQL type: " + mysqlType);
                    writer.println("Oracle type: " + oracleType);
                }


            }
        } catch (JSONException e) {
            e.printStackTrace();
        }
        writer.close();
    }

    private static Properties readPropertiesFile(String fileName) throws IOException {
        Properties properties = new Properties();
        try (InputStream inputStream = Main.class.getClassLoader().getResourceAsStream(fileName)) {
            properties.load(inputStream);
        }
        return properties;
    }

    private static void csvReport(JSONObject mysqlJson, JSONObject oracleJson, String tableName, PrintWriter writer) throws IOException {
        log4j.info("Start comparing table " + tableName);

        String oracleColumnName;
        String mysqlColumnName;
        String oracleDataType;
        String mysqlDataType;
        String oracleDefault;
        String mysqlDefault;
        String oracleIsNullable;
        String mysqlIsNullable;
        String[] oracleTypes = new String[0];
        Map<String, String[]> dataTypeMapping = DataTypeMapper.getDataTypeMapping();
        PrintWriter writer3 = new PrintWriter(new FileWriter("error_table.txt"));
        try {
            JSONArray mysqlArray = mysqlJson.getJSONArray(tableName.toLowerCase());
            JSONArray oracleArray = oracleJson.getJSONArray(tableName);

            // Assuming both arrays are of equal length
            if (mysqlArray.length() != oracleArray.length()) {
                log4j.warning("Table " + tableName + " has different number of columns");
                writer3.append(tableName + " has different number of columns" + "\n");
            } else {
                for (int i = 0; i < mysqlArray.length(); i++) {
                    writer.print(tableName + ",");
                    JSONObject mysqlObject = mysqlArray.getJSONObject(i);
                    JSONObject oracleObject = oracleArray.getJSONObject(i);
                    try {
                        oracleColumnName = oracleObject.getString("Column Name");
                        writer.print(oracleColumnName + ",");
                    } catch (Exception e) {
                        oracleColumnName = null;
                        writer.print(oracleColumnName + ",");
                    }
                    try {
                        oracleDataType = oracleObject.getString("Data Type");
                        writer.print(oracleDataType + ",");
                    } catch (Exception e) {
                        oracleDataType = null;
                        writer.print(oracleDataType + ",");
                    }
                    try {
                        oracleDefault = oracleObject.getString("Data Size");
                        writer.print(oracleDefault + ",");
                    } catch (Exception e) {
                        oracleDefault = null;
                        writer.print(oracleDefault + ",");
                    }
                    try {
                        oracleIsNullable = oracleObject.getString("Is Nullable");
                        writer.print(oracleIsNullable + ",");
                    } catch (Exception e) {
                        oracleIsNullable = null;
                        writer.print(oracleIsNullable + ",");
                    }
                    try {
                        mysqlColumnName = mysqlObject.getString("Column Name");
                        writer.print(mysqlColumnName + ",");
                    } catch (Exception e) {
                        mysqlColumnName = null;
                        writer.print(mysqlColumnName + ",");
                    }
                    try {
                        mysqlDataType = mysqlObject.getString("Data Type").toUpperCase();
                        writer.print(mysqlDataType + ",");
                    } catch (Exception e) {
                        mysqlDataType = null;
                        writer.print(mysqlDataType + ",");

                    }
                    try {
                        mysqlDefault = mysqlObject.getString("Column Default");
                        writer.print(mysqlDefault + ",");
                    } catch (Exception e) {
                        mysqlDefault = null;
                        writer.print(mysqlDefault + ",");

                    }
                    try {
                        mysqlIsNullable = mysqlObject.getString("Is Nullable").substring(0, 1);
                        writer.print(mysqlIsNullable + ",");
                    } catch (Exception e) {
                        mysqlIsNullable = null;
                        writer.print(mysqlIsNullable + ",");
                    }

                    //validate data
                    if (oracleColumnName != null & mysqlColumnName != null) {
                        if (!oracleColumnName.equals(mysqlColumnName)) {
                            writer.print("FALSE" + ",");
                        } else {
                            writer.print("TRUE" + ",");
                        }
                    } else {
                        writer.print("N/A" + ",");
                    }

                    if (oracleDataType != null & mysqlDataType != null) {
                        oracleTypes = dataTypeMapping.get(oracleDataType);
                        if (oracleTypes == null) {
                            writer.print("N/A" + ",");
                        } else if (Arrays.stream(oracleTypes).noneMatch(mysqlDataType::equals)) {
                            writer.print("FALSE" + ",");
                        } else {
                            writer.print("TRUE" + ",");
                        }
                    } else {
                        writer.print("N/A" + ",");
                    }

                    if (oracleDefault != null & mysqlDefault != null) {
                        if (!oracleDefault.equals(mysqlDefault)) {
                            writer.print("FALSE" + ",");
                        } else {
                            writer.print("TRUE" + ",");
                        }
                    } else {
                        writer.print("N/A" + ",");
                    }

                    if (oracleIsNullable != null & mysqlIsNullable != null) {
                        if (oracleIsNullable.equals(mysqlIsNullable)) {
                            writer.print("TRUE" + ",");
                        } else {
                            writer.print("FALSE" + ",");
                        }
                    } else {
                        writer.print("N/A" + ",");
                    }

                    if (oracleTypes != null) {
                        for (String oracleType : oracleTypes) {
                            writer.print(oracleType + " ");
                        }
                    } else {
                        writer.print("N/A");
                    }

                    writer.println();
                }
            }
        } catch (JSONException e) {
            e.printStackTrace();
        }
        writer3.close();
    }
}


