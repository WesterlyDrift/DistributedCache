package com.kvstore.core.storage.LSMTree;

import com.kvstore.core.storage.LSMTree.tree.LSMTree;
import com.kvstore.core.storage.LSMTree.types.ByteArrayPair;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import java.util.Scanner;
import java.util.stream.Stream;

public class LSMTreeTest {
    static final String DIRECTORY = "LSM-data";

    public static void main(String[] args) throws InterruptedException {

        if(new File(DIRECTORY).exists()) {
            deleteDir();
        }

        LSMTree tree = new LSMTree(1024 * 1024, 2, DIRECTORY);

        Scanner scanner = new Scanner(System.in);
        scanner.useDelimiter("\n");

        String intro = """
                  
                  |      __|   \\  |           __ __|             \s
                  |    \\__ \\  |\\/ |   ____|      |   _| -_)   -_)\s
                 ____| ____/ _|  _|             _| _| \\___| \\___|\s
                """;

        String help = """
                Commands:
                  - s/set  <key> <value> : insert a key-value pair;
                  - r/range  <start> <end> : insert this range of numeric keys with random values;
                  - g/get  <key>         : get a key value;
                  - d/del  <key>         : delete a key;
                  - p/prt                : print current tree status;
                  - e/exit               : stop the console;
                  - h/help               : show this message.
                """;

        System.out.println(intro);
        System.out.println(help);

        Random random = new Random();

        boolean exit = false;

        while(!exit) {
            System.out.print("> ");
            String command = scanner.nextLine();

            try {
                String[] parts = command.split(" ");

                switch(parts[0]) {
                    case "s", "set" -> {
                        tree.add(new ByteArrayPair(parts[1].getBytes(), parts[2].getBytes()));
                        System.out.printf("New pair set: {%s : %s}\n", parts[1], parts[2]);
                    }

                    case "r", "range" -> {
                        IntStream.range(Integer)
                    }
                }
            }
        }
    }

    public static void deleteDir() {
        try (Stream<Path> f = Files.walk(Path.of(DIRECTORY))) {
            f.map(Path::toFile).forEach(File::delete);
        } catch(Exception ignored) {
            return;
        }
    }

    public static byte[] intToBytes(int i) {
        byte[] result = new byte[4];
        result[0] = (byte)(i & 0xFF);
        result[1] = (byte)((i >> 8) & 0xFF);
        result[2] = (byte)((i >> 16) & 0xFF);
        result[3] = (byte)((i >> 24) & 0xFF);

        return result;
    }
}
