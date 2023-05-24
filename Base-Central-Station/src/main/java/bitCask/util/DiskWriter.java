package bitCask.util;

import com.google.common.primitives.Ints;
import bitCask.storage.BitCaskEntry;
import bitCask.storage.EntryMetaData;

import java.io.*;
import java.util.Map;

public class DiskWriter {
    String directoryPath, fileName, fileNameReplica;
    FileOutputStream fileOutputStream, fileOutputStreamReplica;
    File file, fileReplica;

    public DiskWriter(String directoryPath) throws FileNotFoundException {
        this.directoryPath = directoryPath;
        createNewFile();
    }

    public void writeEntryToHintFile(String fileName, EntryMetaData entryMetaData, String key) throws IOException {
        String hintFilePath = directoryPath + Constants.HINT_FILE_PREFIX + fileName;
        File hintFile = new File(hintFilePath);

        if (!hintFile.exists() && hintFile.createNewFile())
            System.out.println("Created hint file: " + hintFilePath);

        FileOutputStream hintFileOutputStream = new FileOutputStream(hintFile, true);
        BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(hintFileOutputStream);

        try {
            byte[] bytesToWrite = entryMetaData.toBytes(key, entryMetaData.getValuePosition());
            bufferedOutputStream.write(Ints.toByteArray(bytesToWrite.length));
            bufferedOutputStream.write(bytesToWrite);
            bufferedOutputStream.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void writeHintFileToDisk(String fileName, Map<String, EntryMetaData> compactedKeyToEntryMetaData) throws IOException {
        for (Map.Entry<String, EntryMetaData> entry : compactedKeyToEntryMetaData.entrySet()) {
            try {
                writeEntryToHintFile(fileName, entry.getValue(), entry.getKey());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public DiskResponse writeCompacted(BitCaskEntry bitCaskEntry, File file) throws IOException {
        File fileReplica = new File(directoryPath + "replica_" + file.getName());

        FileOutputStream compactFileOutputStream = new FileOutputStream(file, true);
        FileOutputStream compactFileOutputStreamReplica = new FileOutputStream(fileReplica, true);
        BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(compactFileOutputStream);
        BufferedOutputStream bufferedOutputStreamReplica = new BufferedOutputStream(compactFileOutputStreamReplica);

        byte[] bytesToWrite = bitCaskEntry.toBytes();
        bufferedOutputStream.write(Ints.toByteArray(bytesToWrite.length));
        bufferedOutputStreamReplica.write(Ints.toByteArray(bytesToWrite.length));

        bufferedOutputStream.flush();
        bufferedOutputStreamReplica.flush();

        long valuePosition = file.length() + 8 + 4 + bitCaskEntry.getKeySize() + 4;

        bufferedOutputStream.write(bytesToWrite);
        bufferedOutputStreamReplica.write(bytesToWrite);

        bufferedOutputStream.flush();
        bufferedOutputStreamReplica.flush();

        return new DiskResponse(file.getName(), valuePosition);
    }

    public DiskResponse writeEntryToActiveFile(BitCaskEntry bitCaskEntry) throws IOException {
        checkIfFileExceededSize();
        return writeEntry(bitCaskEntry);
    }

    private DiskResponse writeEntry(BitCaskEntry bitCaskEntry) throws IOException {
        BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(fileOutputStream);
        BufferedOutputStream bufferedOutputStreamReplica = new BufferedOutputStream(fileOutputStreamReplica);

        byte[] bytesToWrite = bitCaskEntry.toBytes();
        bufferedOutputStream.write(Ints.toByteArray(bytesToWrite.length));
        bufferedOutputStreamReplica.write(Ints.toByteArray(bytesToWrite.length));

        bufferedOutputStream.flush();
        bufferedOutputStreamReplica.flush();

        long valuePosition = file.length() + 8 + 4 + bitCaskEntry.getKeySize() + 4;
        writeEntryToHintFile(fileName, new EntryMetaData(bitCaskEntry.getValueSize(), valuePosition,
                bitCaskEntry.getTimestamp(), fileName), bitCaskEntry.getKey());

        bufferedOutputStream.write(bytesToWrite);
        bufferedOutputStreamReplica.write(bytesToWrite);

        bufferedOutputStream.flush();
        bufferedOutputStreamReplica.flush();

        return new DiskResponse(fileName, valuePosition);
    }

    private void checkIfFileExceededSize() throws IOException {
        if (file.length() >= 1048576L)
            createNewFile();
    }

    private void createNewFile() throws FileNotFoundException {
        long timestamp = System.currentTimeMillis();
        fileName = Constants.FILE_PREFIX + timestamp;
        fileNameReplica = Constants.REPLICA_FILE_PREFIX + timestamp;

        String filePath = directoryPath + fileName;
        String replicaFilePath = directoryPath + fileNameReplica;

        fileOutputStream = new FileOutputStream(filePath, true);
        fileOutputStreamReplica = new FileOutputStream(replicaFilePath, true);
        this.file = new File(filePath);
        this.fileReplica = new File(replicaFilePath);
    }
}
