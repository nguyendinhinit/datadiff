package vn.bnh.datadiff.service;

import java.util.ArrayList;

public interface ValidatorService {
    public ArrayList<String> validateTableList(ArrayList<String> source, ArrayList<String> desc, String schema);
}
