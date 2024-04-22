package vn.bnh.datadiff.services;

import vn.bnh.datadiff.dto.ColumnObject;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

public interface Processor {
    void compare(LinkedHashMap<String, Map<String, ArrayList<ColumnObject>>> srcDbMetadata, LinkedHashMap<String, Map<String, ArrayList<ColumnObject>>> destDbMetadata);
}
