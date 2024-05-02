package vn.bnh.datadiff.config;

public class FileHeader {
    private String header;

    static String getFileHeader(String fileType) {
        switch (fileType) {
            case "report":
                return null;
            case "schema level":
                return (String) "abc";
            case "table level":
                return "xyz";

        }
        return null;
    }
}
