package bitCask.command;

import bitCask.exception.DirectoryNotFoundException;
import bitCask.storage.BitCask;
import com.google.common.primitives.Ints;

import java.io.IOException;
import java.util.Arrays;

public record GetCommand(String key) implements Command {
    @Override
    public String execute(BitCask bitCask) {
        try {
            return Arrays.toString(bitCask.get(Ints.toByteArray(Integer.parseInt(key))));
        } catch (IOException | DirectoryNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
