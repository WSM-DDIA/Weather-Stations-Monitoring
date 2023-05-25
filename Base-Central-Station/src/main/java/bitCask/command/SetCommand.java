package bitCask.command;

import bitCask.storage.BitCask;

import java.io.IOException;

public record SetCommand(String key, String value) implements Command {
    @Override
    public void execute(BitCask bitCask) {
        try {
            bitCask.put(key, value);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
