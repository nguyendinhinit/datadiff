package org.example;

import java.util.HashMap;
import java.util.Map;

class DataTypeMapper {
    private static final Map<String, String> dataTypeMapping = new HashMap<>();

    static {
        // MySQL to Oracle mappings
        dataTypeMapping.put("VARCHAR", "VARCHAR2");
        // Add more mappings as needed

        // Mysql to Oracle mappings (if required)
        dataTypeMapping.put("BIGINT", "NUMBER");
        dataTypeMapping.put("DATETIME", "DATE");
        dataTypeMapping.put("DECIMAL", "FLOAT");
        dataTypeMapping.put("DOUBLE", "FLOAT");
        dataTypeMapping.put("DOUBLE PRECISION", "FLOAT");
        dataTypeMapping.put("ENUM", "VARCHAR2");
        dataTypeMapping.put("INT", "NUMBER");
        dataTypeMapping.put("INTEGER", "NUMBER");
        dataTypeMapping.put("LONGBLOB", "BLOB");
        dataTypeMapping.put("LONGTEXT", "CLOB");
        dataTypeMapping.put("MEDIUMBLOB", "BLOB");
        dataTypeMapping.put("MEDIUMINT", "NUMBER");
        dataTypeMapping.put("MEDIUMTEXT", "CLOB");
        dataTypeMapping.put("NUMERIC", "NUMBER");
        dataTypeMapping.put("REAL", "FLOAT");
        dataTypeMapping.put("SET", "VARCHAR2");
        dataTypeMapping.put("SMALLINT", "NUMBER");
        dataTypeMapping.put("TEXT", "VARCHAR2");
        dataTypeMapping.put("TIME", "DATE");
        dataTypeMapping.put("TIMESTAMP", "DATE");
        dataTypeMapping.put("TINYBLOB", "RAW");
        dataTypeMapping.put("TINYINT", "INT");
        dataTypeMapping.put("TINYTEXT", "VARCHAR2");
        dataTypeMapping.put("YEAR", "NUMBER");
        // Add more mappings as needed
    }

    public static Map<String, String> getDataTypeMapping() {
        return dataTypeMapping;
    }
}
