package com.jdatabase.storage;

/**
 * 页面抽象类
 * 页面大小：4KB (4096 bytes)
 */
public class Page {
    public static final int PAGE_SIZE = 4096;
    public static final int PAGE_HEADER_SIZE = 16;

    private final int pageId;
    private final byte[] data;
    private boolean dirty;
    private int pinCount;

    public Page(int pageId) {
        this.pageId = pageId;
        this.data = new byte[PAGE_SIZE];
        this.dirty = false;
        this.pinCount = 0;
    }

    public Page(int pageId, byte[] data) {
        this.pageId = pageId;
        this.data = new byte[PAGE_SIZE];
        System.arraycopy(data, 0, this.data, 0, Math.min(data.length, PAGE_SIZE));
        this.dirty = false;
        this.pinCount = 0;
    }

    public int getPageId() {
        return pageId;
    }

    public byte[] getData() {
        return data;
    }

    public boolean isDirty() {
        return dirty;
    }

    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    public void markDirty() {
        this.dirty = true;
    }

    public int getPinCount() {
        return pinCount;
    }

    public void pin() {
        pinCount++;
    }

    public void unpin() {
        if (pinCount > 0) {
            pinCount--;
        }
    }

    public boolean isPinned() {
        return pinCount > 0;
    }

    /**
     * 从页面读取整数
     */
    public int readInt(int offset) {
        return ((data[offset] & 0xFF) << 24) |
               ((data[offset + 1] & 0xFF) << 16) |
               ((data[offset + 2] & 0xFF) << 8) |
               (data[offset + 3] & 0xFF);
    }

    /**
     * 向页面写入整数
     */
    public void writeInt(int offset, int value) {
        data[offset] = (byte) (value >>> 24);
        data[offset + 1] = (byte) (value >>> 16);
        data[offset + 2] = (byte) (value >>> 8);
        data[offset + 3] = (byte) value;
        markDirty();
    }

    /**
     * 从页面读取长整数
     */
    public long readLong(int offset) {
        return ((long) (data[offset] & 0xFF) << 56) |
               ((long) (data[offset + 1] & 0xFF) << 48) |
               ((long) (data[offset + 2] & 0xFF) << 40) |
               ((long) (data[offset + 3] & 0xFF) << 32) |
               ((long) (data[offset + 4] & 0xFF) << 24) |
               ((long) (data[offset + 5] & 0xFF) << 16) |
               ((long) (data[offset + 6] & 0xFF) << 8) |
               ((long) (data[offset + 7] & 0xFF));
    }

    /**
     * 向页面写入长整数
     */
    public void writeLong(int offset, long value) {
        data[offset] = (byte) (value >>> 56);
        data[offset + 1] = (byte) (value >>> 48);
        data[offset + 2] = (byte) (value >>> 40);
        data[offset + 3] = (byte) (value >>> 32);
        data[offset + 4] = (byte) (value >>> 24);
        data[offset + 5] = (byte) (value >>> 16);
        data[offset + 6] = (byte) (value >>> 8);
        data[offset + 7] = (byte) value;
        markDirty();
    }

    /**
     * 从页面读取字节数组
     */
    public void readBytes(int offset, byte[] dest, int destOffset, int length) {
        System.arraycopy(data, offset, dest, destOffset, length);
    }

    /**
     * 向页面写入字节数组
     */
    public void writeBytes(int offset, byte[] src, int srcOffset, int length) {
        System.arraycopy(src, srcOffset, data, offset, length);
        markDirty();
    }
}

