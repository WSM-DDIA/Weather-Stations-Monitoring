package bitCask.util;

public class DirectoryConstants {
    public static final String DataExtension = ".bitcask.data";
    public static final String ReplicaExtension = ".bitcask.replica";
    public static final String HintExtension = ".bitcask.hint";
    public static final String dbDirectory = "/home/bazina/IdeaProjects/Weather-Stations-Monitoring/Base-Central-Station/src/main/resources/";
    public static final long MEMORY_LIMIT = 32768;

    /**
     * Returns the timestamp of the file.
     *
     * @param fileName name of the file
     * @return timestamp of the file
     */
    public static String getFileTimeStamp(String fileName) {
        return fileName.split("\\.")[0];
    }
}
