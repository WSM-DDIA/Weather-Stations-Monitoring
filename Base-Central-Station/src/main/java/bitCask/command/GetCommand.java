package bitCask.command;

import bitCask.exception.DirectoryNotFoundException;
import bitCask.storage.BitCask;
import com.google.common.primitives.Ints;

import java.io.IOException;
import java.util.Arrays;

public record GetCommand(String key) implements Command {
    /**
     * do get command which gets the value of the key
     *
     * @param bitCask the key value store
     * @return byte array of the value as a string
     */
    @Override
    public String execute(BitCask bitCask) {
        try {
            return Arrays.toString(bitCask.get(Ints.toByteArray(Integer.parseInt(key))));
        } catch (IOException | DirectoryNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
