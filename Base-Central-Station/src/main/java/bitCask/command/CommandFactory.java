package bitCask.command;

import bitCask.exception.InvalidCommandException;

public class CommandFactory {

    /**
     * Parses given input String to a concrete {@link Command} implementation
     *
     * @param input String representation of input
     * @return concrete implementation of {@link Command} interface
     * @throws InvalidCommandException If the given command is an invalid command
     */
    public Command parseCommand(String input) throws InvalidCommandException {
        int cursor = 0;
        cursor = cleanWhiteSpace(input, cursor);
        if (cursor + 2 >= input.length()) {
            throw new InvalidCommandException("Operation needs to be specified in the command");
        }
        String operation = input.substring(cursor, cursor + 3).toUpperCase();
        cursor += 3;
        cursor = cleanWhiteSpace(input, cursor);
        if (cursor == input.length()) {
            throw new InvalidCommandException("Operands needs to be specified in the command");
        }

        return switch (operation) {
            case "GET" -> parseGetCommand(input, cursor);
            case "SET" -> parseSetCommand(input, cursor);
            default -> throw new InvalidCommandException("Operation not supported");
        };
    }

    private SetCommand parseSetCommand(String input, int cursor) throws InvalidCommandException {
        StringBuilder sb = new StringBuilder();
        while (cursor < input.length() && input.charAt(cursor) != ' ') {
            sb.append(input.charAt(cursor++));
        }
        String key = sb.toString();
        sb.setLength(0);
        cursor = cleanWhiteSpace(input, cursor);
        if (cursor == input.length()) {
            throw new InvalidCommandException("SET operation should contain value parameter");
        }
        while (cursor < input.length()) {
            sb.append(input.charAt(cursor++));
        }
        String value = sb.toString();
        cursor = cleanWhiteSpace(input, cursor);
        if (cursor != input.length()) {
            throw new InvalidCommandException("SET operation should not contain parameters in addition to key & value");
        }
        return new SetCommand(key, value);
    }

    private GetCommand parseGetCommand(String input, int cursor) throws InvalidCommandException {
        String key = parseKey(input, cursor);
        System.out.println(key);
        return new GetCommand(key);
    }

    private String parseKey(String input, int cursor) throws InvalidCommandException {
        StringBuilder sb = new StringBuilder();
        while (cursor < input.length() && input.charAt(cursor) != ' ') {
            sb.append(input.charAt(cursor++));
        }
        String key = sb.toString();
        cursor = cleanWhiteSpace(input, cursor);
        if (cursor != input.length()) {
            throw new InvalidCommandException("GET/DEL operation should not contain parameters in addition to key");
        }
        return key;
    }

    private int cleanWhiteSpace(String input, int cursor) {
        while (cursor < input.length() && input.charAt(cursor) == ' ') {
            cursor++;
        }
        return cursor;
    }

    public CommandFactory() {
    }
}
