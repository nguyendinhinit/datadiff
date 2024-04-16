package vn.bnh.datadiff.controller;

import org.json.JSONObject;
import vn.bnh.datadiff.service.Impl.ValidatorServiceImpl;

import java.io.FileNotFoundException;
import java.util.ArrayList;

public class ValidatorController {

    ValidatorServiceImpl validatorServiceImpl = new ValidatorServiceImpl();

    public ArrayList<String> validateTableList(ArrayList<String> oracleTableList, ArrayList<String> mysqlTableList, String schema) {
        //find mismatch table list
        return validatorServiceImpl.validateTableList(oracleTableList, mysqlTableList, schema);
    }

    public void createCSVReport(JSONObject source, JSONObject desc, ArrayList<String> soureTableList, ArrayList<String> descTableList, String schemaName) throws FileNotFoundException {
        //create csv report
        validatorServiceImpl.createCSVReport(source, desc, soureTableList, descTableList, schemaName);
    }

    public void validateKey(JSONObject source, JSONObject desc, ArrayList<String> sourceTableList, ArrayList<String> descTableList, String schemaName) throws FileNotFoundException {
        //validate key
        validatorServiceImpl.validateKey(source, desc, sourceTableList, descTableList, schemaName);
    }
}
