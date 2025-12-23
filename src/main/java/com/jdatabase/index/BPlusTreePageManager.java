package com.jdatabase.index;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

/**
 * B+树页面管理器
 */
public class BPlusTreePageManager {
    private final String dataDir;
    private final Map<String, Integer> rootPageIds;

    public BPlusTreePageManager(String dataDir) {
        this.dataDir = dataDir;
        this.rootPageIds = new HashMap<>();
        loadRootPageIds();
    }

    public BPlusTree.BPlusTreeNode readNode(String indexFile, int pageId) throws IOException {
        Path filePath = Paths.get(dataDir, indexFile);
        if (!Files.exists(filePath)) {
            return new BPlusTree.BPlusTreeNode(true);
        }

        try (ObjectInputStream ois = new ObjectInputStream(
                new FileInputStream(filePath.toFile()))) {
            // 读取节点数量
            int nodeCount = ois.readInt();
            // 读取根页面ID（跳过）
            ois.readInt();
            
            // 查找指定页面
            for (int i = 0; i < nodeCount; i++) {
                int id = ois.readInt();
                if (id == pageId) {
                    return (BPlusTree.BPlusTreeNode) ois.readObject();
                } else {
                    // 跳过这个节点
                    ois.readObject();
                }
            }
        } catch (ClassNotFoundException e) {
            throw new IOException("Failed to read node", e);
        }
        
        return new BPlusTree.BPlusTreeNode(true);
    }

    public void writeNode(String indexFile, int pageId, BPlusTree.BPlusTreeNode node) throws IOException {
        Path filePath = Paths.get(dataDir, indexFile);
        Map<Integer, BPlusTree.BPlusTreeNode> nodes = new HashMap<>();
        
        // 读取现有节点
        if (Files.exists(filePath)) {
            try (ObjectInputStream ois = new ObjectInputStream(
                    new FileInputStream(filePath.toFile()))) {
                int nodeCount = ois.readInt();
                ois.readInt(); // 跳过根页面ID
                for (int i = 0; i < nodeCount; i++) {
                    int id = ois.readInt();
                    BPlusTree.BPlusTreeNode n = (BPlusTree.BPlusTreeNode) ois.readObject();
                    nodes.put(id, n);
                }
            } catch (ClassNotFoundException e) {
                // 忽略，创建新文件
            }
        }
        
        // 更新或添加节点
        nodes.put(pageId, node);
        
        // 写入所有节点
        try (ObjectOutputStream oos = new ObjectOutputStream(
                new FileOutputStream(filePath.toFile()))) {
            oos.writeInt(nodes.size());
            oos.writeInt(rootPageIds.getOrDefault(indexFile, -1));
            for (Map.Entry<Integer, BPlusTree.BPlusTreeNode> entry : nodes.entrySet()) {
                oos.writeInt(entry.getKey());
                oos.writeObject(entry.getValue());
            }
        }
    }

    public int allocatePage(String indexFile) throws IOException {
        Path filePath = Paths.get(dataDir, indexFile);
        int pageId = 0;
        
        if (Files.exists(filePath)) {
            try (ObjectInputStream ois = new ObjectInputStream(
                    new FileInputStream(filePath.toFile()))) {
                int nodeCount = ois.readInt();
                pageId = nodeCount; // 使用节点数量作为新页面ID
            } catch (Exception e) {
                // 忽略
            }
        }
        
        return pageId;
    }

    public int getRootPageId(String indexFile) {
        return rootPageIds.getOrDefault(indexFile, -1);
    }

    public void setRootPageId(String indexFile, int pageId) {
        rootPageIds.put(indexFile, pageId);
        saveRootPageIds();
    }

    private void loadRootPageIds() {
        Path rootFile = Paths.get(dataDir, "index_roots.dat");
        if (!Files.exists(rootFile)) {
            return;
        }

        try (ObjectInputStream ois = new ObjectInputStream(
                new FileInputStream(rootFile.toFile()))) {
            @SuppressWarnings("unchecked")
            Map<String, Integer> loaded = (Map<String, Integer>) ois.readObject();
            rootPageIds.putAll(loaded);
        } catch (IOException | ClassNotFoundException e) {
            // 忽略
        }
    }

    private void saveRootPageIds() {
        Path rootFile = Paths.get(dataDir, "index_roots.dat");
        try (ObjectOutputStream oos = new ObjectOutputStream(
                new FileOutputStream(rootFile.toFile()))) {
            oos.writeObject(rootPageIds);
        } catch (IOException e) {
            // 忽略
        }
    }
}

