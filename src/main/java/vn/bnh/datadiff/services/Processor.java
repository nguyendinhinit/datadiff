package vn.bnh.datadiff.services;

import vn.bnh.datadiff.dto.ColumnObject;
import vn.bnh.datadiff.dto.DBObject;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

public interface Processor {
    void compare(LinkedHashMap<String, Map<String, ArrayList<ColumnObject>>> srcDbMetadata, LinkedHashMap<String, Map<String, ArrayList<ColumnObject>>> destDbMetadata);

    Map<String, Integer[]> countConstrainsAndIndexes(DBObject dbObject);

    void printConstrainsAndIndexes(Map<String, Integer[]> srcConstrainsAndIndexes, Map<String, Integer[]> destConstrainsAndIndexes);

}
