package org.example;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

class DataTypeMapper {
    private static final Map<String, String[]> dataTypeMapping = new HashMap<>();

    static {
        // Oracle to Mysql mappings
        dataTypeMapping.put("NUMBER", new String[]{"BIGINT","INT","INTEGER","MEDIUMINT","NUMERIC","SMALLINT","TINYINT","YEAR"});
        dataTypeMapping.put("RAW", new String[]{"BIT","BLOB","LONGBLOB","LONGTEXT","MEDIUMBLOB","MEDIUMTEXT","TINYBLOB"});
        dataTypeMapping.put("BLOB", new String[]{"BLOB","LONGBLOB","MEDIUMBLOB"});
        dataTypeMapping.put("CHAR", new String[]{"CHAR"});
        dataTypeMapping.put("DATE", new String[]{"DATE","DATETIME","TIMESTAMP","TIME"});
        dataTypeMapping.put("FLOAT", new String[]{"DECIMAL","DOUBLE","DOUBLE PRECISION","FLOAT","REAL"});
        dataTypeMapping.put("VARCHAR2", new String[]{"ENUM","SET","TEXT","TINYTEXT","VARCHAR"});
        dataTypeMapping.put("CLOB", new String[]{"LONGTEXT","MEDIUMTEXT","TEXT","VARCHAR"});
        // Add more mappings as needed
    }

    public static Map<String, String []> getDataTypeMapping() {
        return dataTypeMapping;
    }
}
