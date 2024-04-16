package vn.bnh.datadiff.controller;

import org.json.JSONObject;
import vn.bnh.datadiff.dto.DBObject;
import vn.bnh.datadiff.service.ValidatorService;

import java.io.FileNotFoundException;
import java.util.ArrayList;

public class ValidatorController {

    ValidatorService validatorService = new ValidatorService();

    public ArrayList<String> validateTableList(ArrayList<String> oracleTableList, ArrayList<String> mysqlTableList, String schema) {
        //find mismatch table list
        return validatorService.validateTableList(oracleTableList, mysqlTableList, schema);
    }

    public void createCSVReport(JSONObject source, JSONObject desc, ArrayList<String> soureTableList, ArrayList<String> descTableList, String schemaName) throws FileNotFoundException {
        //create csv report
        validatorService.createCSVReport(source, desc, soureTableList, descTableList, schemaName);
    }

    public void validateKey(JSONObject source, JSONObject desc, ArrayList<String> sourceTableList, ArrayList<String> descTableList, String schemaName) throws FileNotFoundException {
        //validate key
        validatorService.validateKey(source, desc, sourceTableList, descTableList, schemaName);
    }
}
