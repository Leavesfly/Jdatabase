package com.jdatabase.index;

import com.jdatabase.storage.RecordId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * B+树索引
 */
public class BPlusTree {
    private static final int MAX_KEYS = 100; // 每个节点的最大键数
    private final String indexFile;
    private final BPlusTreePageManager pageManager;
    private int rootPageId;

    public BPlusTree(String indexFile, BPlusTreePageManager pageManager) throws IOException {
        this.indexFile = indexFile;
        this.pageManager = pageManager;
        this.rootPageId = pageManager.getRootPageId(indexFile);
        if (rootPageId < 0) {
            // 创建根节点
            BPlusTreeNode root = new BPlusTreeNode(true);
            rootPageId = pageManager.allocatePage(indexFile);
            pageManager.writeNode(indexFile, rootPageId, root);
            pageManager.setRootPageId(indexFile, rootPageId);
        }
    }

    /**
     * 插入键值对
     */
    public void insert(Comparable<?> key, RecordId recordId) throws IOException {
        BPlusTreeNode root = pageManager.readNode(indexFile, rootPageId);
        InsertResult result = insertInternal(root, key, recordId);
        
        if (result.newKey != null) {
            // 根节点分裂，创建新根
            BPlusTreeNode newRoot = new BPlusTreeNode(false);
            newRoot.keys.add(result.newKey);
            newRoot.children.add(rootPageId);
            newRoot.children.add(result.newPageId);
            int newRootPageId = pageManager.allocatePage(indexFile);
            pageManager.writeNode(indexFile, newRootPageId, newRoot);
            rootPageId = newRootPageId;
            pageManager.setRootPageId(indexFile, rootPageId);
        }
    }

    /**
     * 查找键对应的记录ID
     */
    public List<RecordId> search(Comparable<?> key) throws IOException {
        return searchInternal(rootPageId, key);
    }

    /**
     * 删除键值对
     */
    public void delete(Comparable<?> key) throws IOException {
        deleteInternal(rootPageId, key);
    }

    private InsertResult insertInternal(BPlusTreeNode node, Comparable<?> key, RecordId recordId) throws IOException {
        if (node.isLeaf) {
            // 叶子节点：插入键值对
            int pos = findInsertPosition(node.keys, key);
            node.keys.add(pos, key);
            node.values.add(pos, recordId);
            
            if (node.keys.size() > MAX_KEYS) {
                // 分裂叶子节点
                return splitLeafNode(node);
            }
            return new InsertResult(null, -1);
        } else {
            // 内部节点：找到子节点
            int childIndex = findChildIndex(node.keys, key);
            int childPageId = node.children.get(childIndex);
            BPlusTreeNode child = pageManager.readNode(indexFile, childPageId);
            InsertResult result = insertInternal(child, key, recordId);
            
            if (result.newKey != null) {
                // 子节点分裂，插入新键
                int insertPos = findInsertPosition(node.keys, result.newKey);
                node.keys.add(insertPos, result.newKey);
                node.children.add(insertPos + 1, result.newPageId);
                
                if (node.keys.size() > MAX_KEYS) {
                    // 分裂内部节点
                    return splitInternalNode(node);
                }
            }
            return new InsertResult(null, -1);
        }
    }

    private List<RecordId> searchInternal(int pageId, Comparable<?> key) throws IOException {
        BPlusTreeNode node = pageManager.readNode(indexFile, pageId);
        
        if (node.isLeaf) {
            // 在叶子节点中查找
            List<RecordId> results = new ArrayList<>();
            for (int i = 0; i < node.keys.size(); i++) {
                if (compareKeys(node.keys.get(i), key) == 0) {
                    results.add(node.values.get(i));
                }
            }
            return results;
        } else {
            // 在内部节点中查找子节点
            int childIndex = findChildIndex(node.keys, key);
            int childPageId = node.children.get(childIndex);
            return searchInternal(childPageId, key);
        }
    }

    private void deleteInternal(int pageId, Comparable<?> key) throws IOException {
        BPlusTreeNode node = pageManager.readNode(indexFile, pageId);
        
        if (node.isLeaf) {
            // 在叶子节点中删除
            for (int i = 0; i < node.keys.size(); i++) {
                if (compareKeys(node.keys.get(i), key) == 0) {
                    node.keys.remove(i);
                    node.values.remove(i);
                    pageManager.writeNode(indexFile, pageId, node);
                    return;
                }
            }
        } else {
            // 在内部节点中查找子节点
            int childIndex = findChildIndex(node.keys, key);
            int childPageId = node.children.get(childIndex);
            deleteInternal(childPageId, key);
        }
    }

    private InsertResult splitLeafNode(BPlusTreeNode node) throws IOException {
        int mid = node.keys.size() / 2;
        Comparable<?> newKey = node.keys.get(mid);
        
        // 创建新节点
        BPlusTreeNode newNode = new BPlusTreeNode(true);
        newNode.keys.addAll(node.keys.subList(mid, node.keys.size()));
        newNode.values.addAll(node.values.subList(mid, node.values.size()));
        
        // 更新原节点
        node.keys.subList(mid, node.keys.size()).clear();
        node.values.subList(mid, node.values.size()).clear();
        
        int newPageId = pageManager.allocatePage(indexFile);
        pageManager.writeNode(indexFile, newPageId, newNode);
        
        return new InsertResult(newKey, newPageId);
    }

    private InsertResult splitInternalNode(BPlusTreeNode node) throws IOException {
        int mid = node.keys.size() / 2;
        Comparable<?> newKey = node.keys.get(mid);
        
        // 创建新节点
        BPlusTreeNode newNode = new BPlusTreeNode(false);
        newNode.keys.addAll(node.keys.subList(mid + 1, node.keys.size()));
        newNode.children.addAll(node.children.subList(mid + 1, node.children.size()));
        
        // 更新原节点
        node.keys.subList(mid, node.keys.size()).clear();
        node.children.subList(mid + 1, node.children.size()).clear();
        
        int newPageId = pageManager.allocatePage(indexFile);
        pageManager.writeNode(indexFile, newPageId, newNode);
        
        return new InsertResult(newKey, newPageId);
    }

    private int findInsertPosition(List<Comparable<?>> keys, Comparable<?> key) {
        int pos = 0;
        while (pos < keys.size() && compareKeys(keys.get(pos), key) < 0) {
            pos++;
        }
        return pos;
    }

    private int findChildIndex(List<Comparable<?>> keys, Comparable<?> key) {
        int index = 0;
        while (index < keys.size() && compareKeys(keys.get(index), key) <= 0) {
            index++;
        }
        return index;
    }

    @SuppressWarnings("unchecked")
    private int compareKeys(Comparable<?> k1, Comparable<?> k2) {
        return ((Comparable<Object>) k1).compareTo(k2);
    }

    private static class InsertResult {
        final Comparable<?> newKey;
        final int newPageId;

        InsertResult(Comparable<?> newKey, int newPageId) {
            this.newKey = newKey;
            this.newPageId = newPageId;
        }
    }

    /**
     * B+树节点
     */
    public static class BPlusTreeNode {
        boolean isLeaf;
        List<Comparable<?>> keys;
        List<Integer> children; // 内部节点使用
        List<RecordId> values; // 叶子节点使用

        public BPlusTreeNode(boolean isLeaf) {
            this.isLeaf = isLeaf;
            this.keys = new ArrayList<>();
            if (isLeaf) {
                this.values = new ArrayList<>();
            } else {
                this.children = new ArrayList<>();
            }
        }
    }
}

