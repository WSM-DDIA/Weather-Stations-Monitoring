package bitCask.command;

import bitCask.exception.DirectoryNotFoundException;
import bitCask.storage.BitCask;
import com.google.common.primitives.Ints;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public record SetCommand(String key, String value) implements Command {
    @Override
    public String execute(BitCask bitCask) {
        try {
            bitCask.put(Ints.toByteArray(Integer.parseInt(key)),
                    value.getBytes(StandardCharsets.UTF_8));
            return "OK";
        } catch (IOException | DirectoryNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
