package vn.bnh.datadiff.controller;

import org.json.JSONObject;
import vn.bnh.datadiff.service.Impl.ValidatorServiceImpl;
import vn.bnh.datadiff.service.ValidatorService;

import java.io.FileNotFoundException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Map;

public class ValidatorController {

    ValidatorService validatorService = new ValidatorServiceImpl();

    @Deprecated
    public ArrayList<String> validateTableList(ArrayList<String> oracleTableList, ArrayList<String> mysqlTableList, String schema) {
        //find mismatch table list
        return validatorService.validateTableList(oracleTableList, mysqlTableList, schema);
    }

    public ArrayList<String> validateTableList(Map<String,String> informationMap, int compareOption) {
        //find mismatch table list
        return validatorService.validateTableList(informationMap, compareOption);
    }

    @Deprecated
    public void createCSVReport(JSONObject source, JSONObject desc, ArrayList<String> soureTableList, ArrayList<String> descTableList, String schemaName) throws FileNotFoundException {
        //create csv report
        validatorService.createCSVReport(source, desc, soureTableList, descTableList, schemaName);
    }

    public void createCSVReport(String table,String sourceConnectionString,String source_username,String source_password,String descConnectionString,String desc_username,String desc_password ) {
        //create csv report
        validatorService.createCSVReport(table,sourceConnectionString,source_username,source_password,descConnectionString,desc_username,desc_password);
    }

    public void validateKey(JSONObject source, JSONObject desc, ArrayList<String> sourceTableList, ArrayList<String> descTableList, String schemaName) throws FileNotFoundException {
        //validate key
        validatorService.validateKey(source, desc, sourceTableList, descTableList, schemaName);
    }
}
