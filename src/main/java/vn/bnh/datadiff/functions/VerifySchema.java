package vn.bnh.datadiff.functions;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import vn.bnh.datadiff.controllers.FileProcessorController;
import vn.bnh.datadiff.controllers.ObjectCreatorController;
import vn.bnh.datadiff.controllers.QueryController;
import vn.bnh.datadiff.dto.DBObject;

import java.io.File;
import java.sql.SQLException;
import java.util.Properties;

public class VerifySchema {
    static Logger log4j = LogManager.getLogger(VerifySchema.class);
    static FileProcessorController fileProcessorController = new FileProcessorController();
    static ObjectCreatorController objectCreatorController = new ObjectCreatorController();

    static QueryController queryController = new QueryController();

    public static void run(File file) throws SQLException {
        System.out.println(" _____                                                                         _____ \n" + "( ___ )-----------------------------------------------------------------------( ___ )\n" + " |   |                                                                         |   | \n" + " |   |                                                                         |   | \n" + " |   |   ___      ___ _______   ________  ___  ________ ___    ___             |   | \n" + " |   |  |\\  \\    /  /|\\  ___ \\ |\\   __  \\|\\  \\|\\  _____|\\  \\  /  /|            |   | \n" + " |   |  \\ \\  \\  /  / \\ \\   __/|\\ \\  \\|\\  \\ \\  \\ \\  \\__/\\ \\  \\/  / /            |   | \n" + " |   |   \\ \\  \\/  / / \\ \\  \\_|/_\\ \\   _  _\\ \\  \\ \\   __\\\\ \\    / /             |   | \n" + " |   |    \\ \\    / /   \\ \\  \\_|\\ \\ \\  \\\\  \\\\ \\  \\ \\  \\_| \\/  /  /              |   | \n" + " |   |     \\ \\__/ /     \\ \\_______\\ \\__\\\\ _\\\\ \\__\\ \\__\\__/  / /                |   | \n" + " |   |      \\|__|/       \\|_______|\\|__|\\|__|\\|__|\\|__|\\___/ /                 |   | \n" + " |   |   ________  ________  ___  ___  _______   _____\\______   ________       |   | \n" + " |   |  |\\   ____\\|\\   ____\\|\\  \\|\\  \\|\\  ___ \\ |\\   _ \\  _   \\|\\   __  \\      |   | \n" + " |   |  \\ \\  \\___|\\ \\  \\___|\\ \\  \\\\\\  \\ \\   __/|\\ \\  \\\\\\__\\ \\  \\ \\  \\|\\  \\     |   | \n" + " |   |   \\ \\_____  \\ \\  \\    \\ \\   __  \\ \\  \\_|/_\\ \\  \\\\|__| \\  \\ \\   __  \\    |   | \n" + " |   |    \\|____|\\  \\ \\  \\____\\ \\  \\ \\  \\ \\  \\_|\\ \\ \\  \\    \\ \\  \\ \\  \\ \\  \\   |   | \n" + " |   |      ____\\_\\  \\ \\_______\\ \\__\\ \\__\\ \\_______\\ \\__\\    \\ \\__\\ \\__\\ \\__\\  |   | \n" + " |   |     |\\_________\\|_______|\\|__|\\|__|\\|_______|\\|__|     \\|__|\\|__|\\|__|  |   | \n" + " |   |     \\|_________|                                                        |   | \n" + " |   |                                                                         |   | \n" + " |___|                                                                         |___| \n" + "(_____)-----------------------------------------------------------------------(_____)");
        log4j.info("Start the verifySchema application");
        Properties properties = fileProcessorController.readPropertiesFileV2(file);

        DBObject srcObject = objectCreatorController.create(properties, "src");
        DBObject destObject = objectCreatorController.create(properties, "dest");
        queryController.fullScan(srcObject);
    }
}
