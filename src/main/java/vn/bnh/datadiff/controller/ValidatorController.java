package vn.bnh.datadiff.controller;

import vn.bnh.datadiff.service.ValidatorService;

import java.util.ArrayList;

public class ValidatorController {

    ValidatorService validatorService = new ValidatorService();

    public ArrayList<String> validateTableList(ArrayList<String> oracleTableList, ArrayList<String> mysqlTableList) {
        //find mismatch table list

        return validatorService.validateTableList(oracleTableList, mysqlTableList);
    }
}
