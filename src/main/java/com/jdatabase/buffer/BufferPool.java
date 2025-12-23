package com.jdatabase.buffer;

import com.jdatabase.storage.Page;
import com.jdatabase.storage.PageManager;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 缓冲池（LRU替换策略）
 */
public class BufferPool {
    private final int capacity;
    private final PageManager pageManager;
    private final Map<Integer, Page> pages;
    private final Map<Integer, String> keyToFileName; // key到文件名的映射
    private final LinkedHashMap<Integer, Long> accessOrder; // LRU跟踪
    private final ReadWriteLock lock;
    private long accessCounter;

    public BufferPool(int capacity, PageManager pageManager) {
        this.capacity = capacity;
        this.pageManager = pageManager;
        this.pages = new HashMap<>();
        this.keyToFileName = new HashMap<>();
        this.accessOrder = new LinkedHashMap<>(16, 0.75f, true);
        this.lock = new ReentrantReadWriteLock();
        this.accessCounter = 0;
    }

    /**
     * 获取页面（带读锁）
     */
    public Page getPage(String fileName, int pageId) throws IOException {
        lock.readLock().lock();
        try {
            int key = getKey(fileName, pageId);
            Page page = pages.get(key);
            
            if (page != null) {
                updateAccessOrder(key);
                return page;
            }
        } finally {
            lock.readLock().unlock();
        }

        // 页面不在缓冲池中，需要加载
        lock.writeLock().lock();
        try {
            int key = getKey(fileName, pageId);
            Page page = pages.get(key);
            
            if (page != null) {
                updateAccessOrder(key);
                return page;
            }

            // 如果缓冲池已满，需要替换
            if (pages.size() >= capacity) {
                evictPage();
            }

            // 加载页面
            page = pageManager.readPage(fileName, pageId);
            pages.put(key, page);
            keyToFileName.put(key, fileName);
            updateAccessOrder(key);
            return page;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 标记页面为脏页
     */
    public void markDirty(String fileName, int pageId) {
        lock.writeLock().lock();
        try {
            int key = getKey(fileName, pageId);
            Page page = pages.get(key);
            if (page != null) {
                page.markDirty();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 刷新所有脏页
     */
    public void flushAll() throws IOException {
        lock.writeLock().lock();
        try {
            for (Map.Entry<Integer, Page> entry : pages.entrySet()) {
                Page page = entry.getValue();
                if (page.isDirty()) {
                    String fileName = getFileName(entry.getKey());
                    pageManager.writePage(fileName, page);
                    page.setDirty(false);
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 刷新指定文件的脏页
     */
    public void flushFile(String fileName) throws IOException {
        lock.writeLock().lock();
        try {
            for (Map.Entry<Integer, Page> entry : pages.entrySet()) {
                if (getFileName(entry.getKey()).equals(fileName)) {
                    Page page = entry.getValue();
                    if (page.isDirty()) {
                        pageManager.writePage(fileName, page);
                        page.setDirty(false);
                    }
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 移除页面
     */
    public void removePage(String fileName, int pageId) throws IOException {
        lock.writeLock().lock();
        try {
            int key = getKey(fileName, pageId);
            Page page = pages.remove(key);
            if (page != null && page.isDirty()) {
                pageManager.writePage(fileName, page);
            }
            keyToFileName.remove(key);
            accessOrder.remove(key);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 清空缓冲池
     */
    public void clear() throws IOException {
        flushAll();
        lock.writeLock().lock();
        try {
            pages.clear();
            keyToFileName.clear();
            accessOrder.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void evictPage() throws IOException {
        // LRU: 移除最久未访问的页面
        if (accessOrder.isEmpty()) {
            return;
        }

        Integer lruKey = accessOrder.entrySet().iterator().next().getKey();
        Page page = pages.get(lruKey);
        
        if (page != null) {
            if (page.isDirty()) {
                String fileName = getFileName(lruKey);
                pageManager.writePage(fileName, page);
            }
            pages.remove(lruKey);
            keyToFileName.remove(lruKey);
            accessOrder.remove(lruKey);
        }
    }

    private void updateAccessOrder(int key) {
        accessOrder.put(key, ++accessCounter);
    }

    private int getKey(String fileName, int pageId) {
        return Objects.hash(fileName, pageId);
    }

    private String getFileName(int key) {
        return keyToFileName.getOrDefault(key, "unknown");
    }
}

