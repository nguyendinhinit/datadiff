package vn.bnh.datadiff;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class Main {
    static Logger log4j = LogManager.getLogger(Main.class);
    public static void main(String[] args) {
      try{
        String fileName = args[0];
        String feature = args[1];
        Application application = new Application();
        switch (feature) {
            case "validate":
                application.runValidateMetadata(fileName);
                break;
            case "count job":
                application.runCountJob(fileName);
                break;
        }



      }catch (Exception e){
          log4j.error("Not enough parameter.");
      }

    }
}