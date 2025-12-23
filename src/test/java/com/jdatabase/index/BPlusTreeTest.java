package com.jdatabase.index;

import com.jdatabase.storage.RecordId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * B+树索引测试
 */
public class BPlusTreeTest {
    private BPlusTreePageManager pageManager;
    private String indexFile;

    @BeforeEach
    void setUp(@TempDir Path tempDir) {
        pageManager = new BPlusTreePageManager(tempDir.toString());
        indexFile = "test_index.idx";
    }

    @Test
    void testInsertAndSearch() throws Exception {
        BPlusTree tree = new BPlusTree(indexFile, pageManager);
        
        // 插入键值对
        tree.insert(1, new RecordId(0, 0));
        tree.insert(2, new RecordId(0, 1));
        tree.insert(3, new RecordId(0, 2));
        
        // 搜索
        List<RecordId> result1 = tree.search(1);
        assertNotNull(result1);
        assertFalse(result1.isEmpty());
        
        List<RecordId> result2 = tree.search(2);
        assertNotNull(result2);
        assertFalse(result2.isEmpty());
    }

    @Test
    void testSearchNonExistent() throws Exception {
        BPlusTree tree = new BPlusTree(indexFile, pageManager);
        
        tree.insert(1, new RecordId(0, 0));
        
        // 搜索不存在的键
        List<RecordId> result = tree.search(999);
        assertTrue(result == null || result.isEmpty());
    }

    @Test
    void testDelete() throws Exception {
        BPlusTree tree = new BPlusTree(indexFile, pageManager);
        
        // 插入
        tree.insert(1, new RecordId(0, 0));
        tree.insert(2, new RecordId(0, 1));
        
        // 删除
        tree.delete(1);
        
        // 验证删除
        List<RecordId> result = tree.search(1);
        assertTrue(result == null || result.isEmpty());
        
        // 验证其他键仍然存在
        List<RecordId> result2 = tree.search(2);
        assertFalse(result2.isEmpty());
    }

    @Test
    void testMultipleInserts() throws Exception {
        BPlusTree tree = new BPlusTree(indexFile, pageManager);
        
        // 插入多个键
        for (int i = 0; i < 10; i++) {
            tree.insert(i, new RecordId(0, i));
        }
        
        // 验证所有键都能找到
        for (int i = 0; i < 10; i++) {
            List<RecordId> result = tree.search(i);
            assertFalse(result.isEmpty());
        }
    }
}

