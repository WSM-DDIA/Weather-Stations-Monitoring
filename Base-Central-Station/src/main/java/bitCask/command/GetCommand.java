package bitCask.command;

import bitCask.storage.BitCask;

import java.io.IOException;

public record GetCommand(String key) implements Command {
    @Override
    public void execute(BitCask bitCask) {
        try {
            System.out.println(bitCask.get(key));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
