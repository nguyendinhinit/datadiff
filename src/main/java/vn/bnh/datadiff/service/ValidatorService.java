package vn.bnh.datadiff.service;

import org.json.JSONObject;

import java.io.FileNotFoundException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

public interface ValidatorService {
    @Deprecated
    public ArrayList<String> validateTableList(ArrayList<String> source, ArrayList<String> desc, String schema);

    public ArrayList<String> validateTableList(Map<String, String> informationMap, int compareOption);

    @Deprecated
    public void createCSVReport(JSONObject source, JSONObject desc, ArrayList<String> sourceTableList, ArrayList<String> descTableList, String schemaName) throws FileNotFoundException;

    public void createCSVReport(String table,String sourceConnectionString,String source_username,String source_password,String descConnectionString,String desc_username,String desc_password);

    public void validateKey (JSONObject source, JSONObject desc, ArrayList<String> sourceTableList, ArrayList<String> descTableList, String schemaName);

    public boolean validateColumn(String src, String desc);
}
