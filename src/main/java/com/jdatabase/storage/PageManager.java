package com.jdatabase.storage;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * 页面管理器，负责页面的磁盘I/O
 */
public class PageManager {
    private final String dataDir;

    public PageManager(String dataDir) {
        this.dataDir = dataDir;
        ensureDataDir();
    }

    private void ensureDataDir() {
        try {
            Path dir = Paths.get(dataDir);
            if (!Files.exists(dir)) {
                Files.createDirectories(dir);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to create data directory", e);
        }
    }

    /**
     * 读取页面
     */
    public Page readPage(String fileName, int pageId) throws IOException {
        Path filePath = Paths.get(dataDir, fileName);
        if (!Files.exists(filePath)) {
            return new Page(pageId);
        }

        try (RandomAccessFile file = new RandomAccessFile(filePath.toFile(), "r")) {
            long offset = (long) pageId * Page.PAGE_SIZE;
            if (offset >= file.length()) {
                return new Page(pageId);
            }

            file.seek(offset);
            byte[] data = new byte[Page.PAGE_SIZE];
            int bytesRead = file.read(data);
            if (bytesRead < Page.PAGE_SIZE) {
                // 部分页面，用0填充
                for (int i = bytesRead; i < Page.PAGE_SIZE; i++) {
                    data[i] = 0;
                }
            }
            return new Page(pageId, data);
        }
    }

    /**
     * 写入页面
     */
    public void writePage(String fileName, Page page) throws IOException {
        Path filePath = Paths.get(dataDir, fileName);
        ensureFileExists(filePath);

        try (RandomAccessFile file = new RandomAccessFile(filePath.toFile(), "rw")) {
            long offset = (long) page.getPageId() * Page.PAGE_SIZE;
            file.seek(offset);
            file.write(page.getData());
        }
    }

    /**
     * 分配新页面
     */
    public int allocatePage(String fileName) throws IOException {
        Path filePath = Paths.get(dataDir, fileName);
        ensureFileExists(filePath);

        try (RandomAccessFile file = new RandomAccessFile(filePath.toFile(), "rw")) {
            long fileSize = file.length();
            int pageId = (int) (fileSize / Page.PAGE_SIZE);
            // 扩展文件以包含新页面
            file.setLength((long) (pageId + 1) * Page.PAGE_SIZE);
            return pageId;
        }
    }

    /**
     * 获取文件中的页面数量
     */
    public int getPageCount(String fileName) throws IOException {
        Path filePath = Paths.get(dataDir, fileName);
        if (!Files.exists(filePath)) {
            return 0;
        }

        try (RandomAccessFile file = new RandomAccessFile(filePath.toFile(), "r")) {
            return (int) (file.length() / Page.PAGE_SIZE);
        }
    }

    private void ensureFileExists(Path filePath) throws IOException {
        if (!Files.exists(filePath)) {
            Files.createFile(filePath);
        }
    }

    public String getDataDir() {
        return dataDir;
    }
}

