package com.jdatabase.storage;

import com.jdatabase.common.Schema;
import com.jdatabase.common.Tuple;
import com.jdatabase.common.Types;
import com.jdatabase.common.Value;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/**
 * 记录管理器，负责在页面中存储和读取记录
 * 页面布局：
 * [页头(16B)] [槽目录(变长)] [记录数据(从后往前)]
 * 页头：freeSpaceOffset(4B) + slotCount(4B) + nextPageId(4B) + reserved(4B)
 */
public class RecordManager {
    private static final int FREE_SPACE_OFFSET = 0;
    private static final int SLOT_COUNT_OFFSET = 4;
    private static final int NEXT_PAGE_ID_OFFSET = 8;
    private static final int SLOT_SIZE = 8; // slotId(4B) + offset(4B)

    private final PageManager pageManager;

    public RecordManager(PageManager pageManager) {
        this.pageManager = pageManager;
    }

    /**
     * 插入记录
     */
    public RecordId insertRecord(String fileName, Schema schema, Tuple tuple) throws IOException {
        int pageId = findPageWithSpace(fileName, schema, tuple);
        Page page = pageManager.readPage(fileName, pageId);
        
        try {
            RecordId recordId = insertRecordInPage(page, schema, tuple, pageId);
            pageManager.writePage(fileName, page);
            return recordId;
        } finally {
            page.setDirty(false);
        }
    }

    /**
     * 读取记录
     */
    public Tuple readRecord(String fileName, Schema schema, RecordId recordId) throws IOException {
        Page page = pageManager.readPage(fileName, recordId.getPageId());
        return readRecordFromPage(page, schema, recordId);
    }

    /**
     * 更新记录
     */
    public void updateRecord(String fileName, Schema schema, RecordId recordId, Tuple newTuple) throws IOException {
        Page page = pageManager.readPage(fileName, recordId.getPageId());
        updateRecordInPage(page, schema, recordId, newTuple);
        pageManager.writePage(fileName, page);
    }

    /**
     * 删除记录
     */
    public void deleteRecord(String fileName, Schema schema, RecordId recordId) throws IOException {
        Page page = pageManager.readPage(fileName, recordId.getPageId());
        deleteRecordInPage(page, recordId);
        pageManager.writePage(fileName, page);
    }

    /**
     * 扫描所有记录
     */
    public List<Tuple> scanRecords(String fileName, Schema schema) throws IOException {
        List<Tuple> records = new ArrayList<>();
        int pageCount = pageManager.getPageCount(fileName);
        
        for (int pageId = 0; pageId < pageCount; pageId++) {
            Page page = pageManager.readPage(fileName, pageId);
            int slotCount = page.readInt(SLOT_COUNT_OFFSET);
            
            for (int slotId = 0; slotId < slotCount; slotId++) {
                int slotOffset = Page.PAGE_HEADER_SIZE + slotId * SLOT_SIZE;
                int recordOffset = page.readInt(slotOffset + 4);
                
                if (recordOffset > 0) { // 有效记录
                    RecordId recordId = new RecordId(pageId, slotId);
                    Tuple tuple = readRecordFromPage(page, schema, recordId);
                    if (tuple != null) {
                        records.add(tuple);
                    }
                }
            }
        }
        
        return records;
    }

    private int findPageWithSpace(String fileName, Schema schema, Tuple tuple) throws IOException {
        int pageCount = pageManager.getPageCount(fileName);
        
        // 先尝试在现有页面中找空间
        for (int pageId = 0; pageId < pageCount; pageId++) {
            Page page = pageManager.readPage(fileName, pageId);
            int recordSize = calculateRecordSize(schema, tuple);
            if (hasSpace(page, recordSize)) {
                return pageId;
            }
        }
        
        // 分配新页面
        return pageManager.allocatePage(fileName);
    }

    private boolean hasSpace(Page page, int recordSize) {
        int freeSpaceOffset = page.readInt(FREE_SPACE_OFFSET);
        int slotCount = page.readInt(SLOT_COUNT_OFFSET);
        int slotDirectorySize = slotCount * SLOT_SIZE;
        int usedSpace = Page.PAGE_HEADER_SIZE + slotDirectorySize + (Page.PAGE_SIZE - freeSpaceOffset);
        int availableSpace = Page.PAGE_SIZE - usedSpace;
        return availableSpace >= recordSize + SLOT_SIZE; // 需要空间存储记录和槽
    }

    private RecordId insertRecordInPage(Page page, Schema schema, Tuple tuple, int pageId) {
        int recordSize = calculateRecordSize(schema, tuple);
        int freeSpaceOffset = page.readInt(FREE_SPACE_OFFSET);
        int slotCount = page.readInt(SLOT_COUNT_OFFSET);
        
        // 检查空间
        if (!hasSpace(page, recordSize)) {
            throw new RuntimeException("Page has no space for record");
        }
        
        // 写入记录（从后往前）
        int recordOffset = freeSpaceOffset - recordSize;
        writeRecordToPage(page, schema, tuple, recordOffset);
        
        // 添加槽
        int slotOffset = Page.PAGE_HEADER_SIZE + slotCount * SLOT_SIZE;
        page.writeInt(slotOffset, slotCount); // slotId
        page.writeInt(slotOffset + 4, recordOffset);
        
        // 更新页头
        page.writeInt(FREE_SPACE_OFFSET, recordOffset);
        page.writeInt(SLOT_COUNT_OFFSET, slotCount + 1);
        
        return new RecordId(pageId, slotCount);
    }

    private Tuple readRecordFromPage(Page page, Schema schema, RecordId recordId) {
        int slotOffset = Page.PAGE_HEADER_SIZE + recordId.getSlotId() * SLOT_SIZE;
        int recordOffset = page.readInt(slotOffset + 4);
        
        if (recordOffset <= 0) {
            return null; // 已删除的记录
        }
        
        return readRecordFromOffset(page, schema, recordOffset);
    }

    private void updateRecordInPage(Page page, Schema schema, RecordId recordId, Tuple newTuple) {
        int slotOffset = Page.PAGE_HEADER_SIZE + recordId.getSlotId() * SLOT_SIZE;
        int oldRecordOffset = page.readInt(slotOffset + 4);
        
        if (oldRecordOffset <= 0) {
            throw new RuntimeException("Record not found");
        }
        
        // 删除旧记录（标记为已删除）
        page.writeInt(slotOffset + 4, -oldRecordOffset);
        
        // 插入新记录
        int newRecordSize = calculateRecordSize(schema, newTuple);
        int freeSpaceOffset = page.readInt(FREE_SPACE_OFFSET);
        int newRecordOffset = freeSpaceOffset - newRecordSize;
        
        writeRecordToPage(page, schema, newTuple, newRecordOffset);
        page.writeInt(slotOffset + 4, newRecordOffset);
        page.writeInt(FREE_SPACE_OFFSET, newRecordOffset);
    }

    private void deleteRecordInPage(Page page, RecordId recordId) {
        int slotOffset = Page.PAGE_HEADER_SIZE + recordId.getSlotId() * SLOT_SIZE;
        int recordOffset = page.readInt(slotOffset + 4);
        
        if (recordOffset > 0) {
            // 标记为已删除（使用负偏移量）
            page.writeInt(slotOffset + 4, -recordOffset);
        }
    }

    private void writeRecordToPage(Page page, Schema schema, Tuple tuple, int offset) {
        int currentOffset = offset;
        
        // 写入NULL位图
        BitSet nullBitmap = new BitSet(schema.getColumnCount());
        byte[] nullBytes = new byte[(schema.getColumnCount() + 7) / 8];
        for (int i = 0; i < schema.getColumnCount(); i++) {
            Value value = tuple.getValue(i);
            if (value == null || value.getValue() == null) {
                nullBitmap.set(i);
            }
        }
        nullBitmap.toByteArray();
        System.arraycopy(nullBitmap.toByteArray(), 0, nullBytes, 0, 
                        Math.min(nullBitmap.toByteArray().length, nullBytes.length));
        page.writeBytes(currentOffset, nullBytes, 0, nullBytes.length);
        currentOffset += nullBytes.length;
        
        // 写入列数据
        for (int i = 0; i < schema.getColumnCount(); i++) {
            Value value = tuple.getValue(i);
            if (!nullBitmap.get(i) && value != null) {
                currentOffset = writeValue(page, schema.getColumn(i), value, currentOffset);
            }
        }
    }

    private Tuple readRecordFromOffset(Page page, Schema schema, int offset) {
        Tuple tuple = new Tuple(schema);
        int currentOffset = offset;
        
        // 读取NULL位图
        int nullBitmapSize = (schema.getColumnCount() + 7) / 8;
        byte[] nullBytes = new byte[nullBitmapSize];
        page.readBytes(currentOffset, nullBytes, 0, nullBitmapSize);
        BitSet nullBitmap = BitSet.valueOf(nullBytes);
        currentOffset += nullBitmapSize;
        
        // 读取列数据
        for (int i = 0; i < schema.getColumnCount(); i++) {
            if (!nullBitmap.get(i)) {
                Value value = readValue(page, schema.getColumn(i), currentOffset);
                tuple.setValue(i, value);
                currentOffset += getValueSize(schema.getColumn(i), value);
            } else {
                tuple.setValue(i, null);
            }
        }
        
        return tuple;
    }

    private int writeValue(Page page, Schema.Column column, Value value, int offset) {
        Types type = column.getType();
        switch (type) {
            case INT:
                page.writeInt(offset, value.getInt());
                return offset + 4;
            case LONG:
                page.writeLong(offset, value.getLong());
                return offset + 8;
            case FLOAT:
                page.writeInt(offset, Float.floatToIntBits(value.getFloat()));
                return offset + 4;
            case DOUBLE:
                page.writeLong(offset, Double.doubleToLongBits(value.getDouble()));
                return offset + 8;
            case VARCHAR:
                String str = value.getString();
                byte[] strBytes = str.getBytes();
                page.writeInt(offset, strBytes.length);
                page.writeBytes(offset + 4, strBytes, 0, strBytes.length);
                return offset + 4 + strBytes.length;
            case BOOLEAN:
                page.writeBytes(offset, new byte[]{(byte) (value.getBoolean() ? 1 : 0)}, 0, 1);
                return offset + 1;
            default:
                throw new RuntimeException("Unsupported type: " + type);
        }
    }

    private Value readValue(Page page, Schema.Column column, int offset) {
        Types type = column.getType();
        switch (type) {
            case INT:
                return new Value(Types.INT, page.readInt(offset));
            case LONG:
                return new Value(Types.LONG, page.readLong(offset));
            case FLOAT:
                return new Value(Types.FLOAT, Float.intBitsToFloat(page.readInt(offset)));
            case DOUBLE:
                return new Value(Types.DOUBLE, Double.longBitsToDouble(page.readLong(offset)));
            case VARCHAR:
                int length = page.readInt(offset);
                byte[] strBytes = new byte[length];
                page.readBytes(offset + 4, strBytes, 0, length);
                return new Value(Types.VARCHAR, new String(strBytes));
            case BOOLEAN:
                byte[] boolBytes = new byte[1];
                page.readBytes(offset, boolBytes, 0, 1);
                return new Value(Types.BOOLEAN, boolBytes[0] != 0);
            default:
                throw new RuntimeException("Unsupported type: " + type);
        }
    }

    private int getValueSize(Schema.Column column, Value value) {
        Types type = column.getType();
        if (type.isFixedLength()) {
            return type.getSize();
        } else if (type == Types.VARCHAR) {
            String str = value.getString();
            return 4 + str.getBytes().length; // length(4B) + data
        }
        return 0;
    }

    private int calculateRecordSize(Schema schema, Tuple tuple) {
        int size = (schema.getColumnCount() + 7) / 8; // NULL位图
        
        for (int i = 0; i < schema.getColumnCount(); i++) {
            Value value = tuple.getValue(i);
            if (value != null && value.getValue() != null) {
                size += getValueSize(schema.getColumn(i), value);
            }
        }
        
        return size;
    }
}

