package vn.bnh.datadiff;

public class Main {
    public static void main(String[] args) {
        String fileName = args[0];
        String feature = args[1];
        Application application = new Application();
        switch (feature) {
            case "validate":
                application.runValidateMetadata(fileName);
                break;
            case "count job":
                application.runCountJob(fileName);
        }

    }
}