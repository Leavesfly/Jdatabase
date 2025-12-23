package com.jdatabase.storage;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 页面测试
 */
public class PageTest {
    
    @Test
    void testPageCreation() {
        Page page = new Page(0);
        assertEquals(0, page.getPageId());
        assertFalse(page.isDirty());
        assertEquals(0, page.getPinCount());
    }

    @Test
    void testPageReadWriteInt() {
        Page page = new Page(0);
        
        page.writeInt(0, 12345);
        int value = page.readInt(0);
        assertEquals(12345, value);
        assertTrue(page.isDirty());
    }

    @Test
    void testPageReadWriteLong() {
        Page page = new Page(0);
        
        page.writeLong(0, 123456789L);
        long value = page.readLong(0);
        assertEquals(123456789L, value);
    }

    @Test
    void testPageReadWriteBytes() {
        Page page = new Page(0);
        
        byte[] data = "Hello World".getBytes();
        page.writeBytes(0, data, 0, data.length);
        
        byte[] readData = new byte[data.length];
        page.readBytes(0, readData, 0, data.length);
        
        assertArrayEquals(data, readData);
    }

    @Test
    void testPagePinUnpin() {
        Page page = new Page(0);
        
        page.pin();
        assertEquals(1, page.getPinCount());
        assertTrue(page.isPinned());
        
        page.pin();
        assertEquals(2, page.getPinCount());
        
        page.unpin();
        assertEquals(1, page.getPinCount());
        
        page.unpin();
        assertEquals(0, page.getPinCount());
        assertFalse(page.isPinned());
    }

    @Test
    void testPageDirtyFlag() {
        Page page = new Page(0);
        assertFalse(page.isDirty());
        
        page.markDirty();
        assertTrue(page.isDirty());
        
        page.setDirty(false);
        assertFalse(page.isDirty());
    }
}

