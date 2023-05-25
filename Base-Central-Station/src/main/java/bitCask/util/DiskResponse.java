package bitCask.util;

public class DiskResponse {
    String fileName;
    String replicaFileName;
    long valuePosition;

    public DiskResponse(String fileName, long valuePosition) {
        this.fileName = fileName;
        this.replicaFileName = "replica_" + fileName;
        this.valuePosition = valuePosition;
    }

    public String getFileName() {
        return fileName;
    }

    public String getReplicaFileName() {
        return replicaFileName;
    }

    public long getValuePosition() {
        return valuePosition;
    }
}
