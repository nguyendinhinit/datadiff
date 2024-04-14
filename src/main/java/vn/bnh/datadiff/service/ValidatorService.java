package vn.bnh.datadiff.service;

import java.util.ArrayList;

public class ValidatorService {

    public ArrayList<String> validateTableList(ArrayList<String> source, ArrayList<String> desc) {
        //Find mismatch table list between Oracle and Mysql database and return the list of mismatch table names
        ArrayList<String> tableList = new ArrayList<>();
        for (String table : source) {
            if (!desc.contains(table.toUpperCase())) {
                tableList.add(table);
            }
        }
        return tableList;
    }
}
