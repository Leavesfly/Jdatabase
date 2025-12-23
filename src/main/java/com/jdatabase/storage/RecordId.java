package com.jdatabase.storage;

/**
 * 记录ID，由页号和槽号组成
 */
public class RecordId {
    private final int pageId;
    private final int slotId;

    public RecordId(int pageId, int slotId) {
        this.pageId = pageId;
        this.slotId = slotId;
    }

    public int getPageId() {
        return pageId;
    }

    public int getSlotId() {
        return slotId;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        RecordId recordId = (RecordId) obj;
        return pageId == recordId.pageId && slotId == recordId.slotId;
    }

    @Override
    public int hashCode() {
        return 31 * pageId + slotId;
    }

    @Override
    public String toString() {
        return "RecordId(pageId=" + pageId + ", slotId=" + slotId + ")";
    }
}

