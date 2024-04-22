package vn.bnh.datadiff.mapping;

import lombok.Getter;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class DataTypeMapper {
    @Getter
    private static final Map<String, String[]> dataTypeMapping = new HashMap<>();

    static {
        // Load mappings from file on class initialization
        loadMappings();
    }

    private static void loadMappings() {
        try (BufferedReader reader = new BufferedReader(new FileReader("mapping.txt"))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("=");
                if (parts.length == 2) {
                    String oracleType = parts[0].trim();
                    String[] mysqlTypes = parts[1].split(",");
                    for (int i = 0; i < mysqlTypes.length; i++) {
                        mysqlTypes[i] = mysqlTypes[i].trim();
                    }
                    dataTypeMapping.put(oracleType, mysqlTypes);
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
