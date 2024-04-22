package vn.bnh.datadiff.controllers;

import vn.bnh.datadiff.dto.ColumnObject;
import vn.bnh.datadiff.services.Processor;
import vn.bnh.datadiff.services.impl.ProcessorImpl;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

public class ProcessorController {
    Processor processor = new ProcessorImpl();
   public void compare(LinkedHashMap<String, Map<String, ArrayList<ColumnObject>>> srcDbMetadata, LinkedHashMap<String, Map<String, ArrayList<ColumnObject>>> destDbMetadata){
        processor.compare(srcDbMetadata, destDbMetadata);
    }
}
