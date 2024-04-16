package vn.bnh.datadiff.service.Impl;

import java.util.HashMap;
import java.util.Map;

public class DataTypeMapperImpl {
    private static final Map<String, String[]> dataTypeMapping = new HashMap<>();

    static {
        // Oracle to Mysql mappings
        dataTypeMapping.put("CHAR", new String[]{"CHAR"});
        dataTypeMapping.put("CHARACTER", new String[]{"CHARACTER"});
        dataTypeMapping.put("NCHAR", new String[]{"NCHAR"});
        dataTypeMapping.put("VARCHAR", new String[]{"VARCHAR"});
        dataTypeMapping.put("NCHAR VARYING", new String[]{"NCHAR VARYING"});
        dataTypeMapping.put("VARCHAR2", new String[]{"VARCHAR"});
        dataTypeMapping.put("NVARCHAR2", new String[]{"VARCHAR"});
        dataTypeMapping.put("RAW", new String[]{"VARBINARY"});
        dataTypeMapping.put("LONG RAW", new String[]{"LONGTEXT"});
        dataTypeMapping.put("NUMBER", new String[]{"DECIMAL","DOUBLE"});
        dataTypeMapping.put("FLOAT", new String[]{"FLOAT"});
        dataTypeMapping.put("DEC", new String[]{"DEC"});
        dataTypeMapping.put("DECIMAL", new String[]{"DECIMAL"});
        dataTypeMapping.put("INT", new String[]{"INT"});
        dataTypeMapping.put("INTEGER", new String[]{"INTEGER"});
        dataTypeMapping.put("SMALLINT", new String[]{"SMALLINT"});
        dataTypeMapping.put("REAL", new String[]{"REAL"});
        dataTypeMapping.put("DOUBLE PRECISION", new String[]{"DOUBLE PRECISION"});
        dataTypeMapping.put("DATE", new String[]{"DATETIME"});
        dataTypeMapping.put("TIMESTAMP", new String[]{"TIMESTAMP(6)"});
        dataTypeMapping.put("TIMESTAMP(6) WITH TIME ZONE", new String[]{"DATETIME"});
        dataTypeMapping.put("INTERVAL YEAR(6) TO MONTH", new String[]{"VARCHAR"});
        dataTypeMapping.put("INTERVAL DAY(6) TO SECOND(s)", new String[]{"VARCHAR"});
        dataTypeMapping.put("BFILE", new String[]{"VARCHAR"});
        dataTypeMapping.put("BLOB", new String[]{"BLOB"});
        dataTypeMapping.put("CLOB", new String[]{"LONGTEXT"});
        dataTypeMapping.put("NCLOB", new String[]{"LONGTEXT"});
        dataTypeMapping.put("ROWID", new String[]{"CHAR"});
        dataTypeMapping.put("UROWID", new String[]{"VARCHAR"});
        dataTypeMapping.put("XMLTYPE", new String[]{"LONGTEXT"});
        dataTypeMapping.put("BOOLEAN", new String[]{"BOOLEAN"});
        // Add more mappings as needed
    }

    public static Map<String, String []> getDataTypeMapping() {
        return dataTypeMapping;
    }
}
