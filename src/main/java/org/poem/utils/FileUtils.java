package org.poem.utils;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.CRC32;
import java.util.zip.CheckedOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * @author Yorke
 */
public class FileUtils {

    public static void createFile(String filePath, String content) throws IOException {
        Files.write(Paths.get(filePath), Optional.ofNullable(content).orElse("").getBytes());
    }

    public static void zipFiles(String zipFile, String... files) throws IOException {
        try (FileOutputStream fos = new FileOutputStream(zipFile);
             CheckedOutputStream cos = new CheckedOutputStream(fos, new CRC32());
             ZipOutputStream zos = new ZipOutputStream(cos)) {

            Stream.of(files)
                    .map(Paths::get)
                    .filter(Files::exists)
                    .forEach(filePath -> compress(filePath, zos, ""));
        }
    }

    /**
     * 压缩目录或文件
     */
    private static void compress(Path filePath, ZipOutputStream zos, String baseDir) {
        try {
            if (Files.isDirectory(filePath)) {
                // 压缩目录
                Files.list(filePath).forEach(subFilePath -> compress(subFilePath, zos, baseDir + filePath.getFileName().toString() + "/"));
            } else {
                // 压缩文件
                ZipEntry entry = new ZipEntry(baseDir + filePath.getFileName().toString());
                zos.putNextEntry(entry);
                zos.write(Files.readAllBytes(filePath));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
