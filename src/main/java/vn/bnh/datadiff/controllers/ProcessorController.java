package vn.bnh.datadiff.controllers;

import vn.bnh.datadiff.dto.ColumnObject;
import vn.bnh.datadiff.dto.DBObject;
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

   public Map<String, Integer[]>  countConstrainsAndIndexes(DBObject dbObject){
       return processor.countConstrainsAndIndexes(dbObject);
   }

   public void printConstrainsAndIndexes(Map<String, Integer[]> srcConstrainsAndIndexes, Map<String, Integer[]> destConstrainsAndIndexes){
       processor.printConstrainsAndIndexes(srcConstrainsAndIndexes, destConstrainsAndIndexes);
   }
   public void foundMissingTable(DBObject srcDbObject, DBObject destDbObject){
       processor.foundMissingTable(srcDbObject, destDbObject);
   }

   public void objectLevelCompare(Map<String, Map<String, ArrayList<Integer>>> srcDbObject, Map<String, Map<String, ArrayList<Integer>>> destDbObject){
       processor.objectLevelCompare(srcDbObject, destDbObject);
   }
}
