package org.poem.utils;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@SpringBootTest
@RunWith(SpringRunner.class)
public class FileUtilsTest {

    @Test
    public void createFile() throws Exception {
        FileUtils.createFile("D:/dev/test.sql", null);
    }

    @Test
    public void zipFiles() throws Exception {
        FileUtils.zipFiles("D:/dev/zip.zip", "D:/dev/test.txt", "D:/dev/test/");
    }
}