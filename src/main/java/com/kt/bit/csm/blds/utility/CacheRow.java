package com.kt.bit.csm.blds.utility;

import java.util.HashMap;

/**
 * Created with IntelliJ IDEA.
 * User: toto
 * Date: 14. 6. 20
 * Time: 오후 2:51
 * To change this template use File | Settings | File Templates.
 */
public class CacheRow {
    HashMap<String, CacheColumn> cacheColumns;
    int rowIndex = 0;

    public CacheRow(CacheColumn[] cacheColumns){
        this.cacheColumns = new HashMap<String, CacheColumn >();
        for( int i = 0; i < cacheColumns.length ; i++ ){
            this.cacheColumns.put(cacheColumns[i].getName(), cacheColumns[i]);
            this.cacheColumns.put(""+i, cacheColumns[i]);
        }
    }

    public CacheRow(CacheColumn[] cacheColumns, int rowIndex){
        this.cacheColumns = new HashMap<String, CacheColumn >();
        for( int i = 0; i < cacheColumns.length ; i++ ){
            this.cacheColumns.put(cacheColumns[i].getName(), cacheColumns[i]);
            this.cacheColumns.put(""+i, cacheColumns[i]);
        }
        this.rowIndex = rowIndex;
    }

    public CacheColumn getColumn(int idx){
        CacheColumn cacheColumn = null;
        if(cacheColumns != null ){
            cacheColumn = this.cacheColumns.get(""+idx);
        }
        return cacheColumn;
    }

    public CacheColumn getColumn(String name){
        CacheColumn cacheColumn = null;
        if(cacheColumns != null ){
            cacheColumn = this.cacheColumns.get(name);
        }
        return cacheColumn;
    }

    public void setColumn(int idx, CacheColumn cacheColumn){
        if(cacheColumns != null ){
            cacheColumns.put(cacheColumn.getName(), cacheColumn);
            cacheColumns.put(""+idx, cacheColumn);
        }
    }

    int getRowIndex() {
        return rowIndex;
    }

    void setRowIndex(int rowIndex) {
        this.rowIndex = rowIndex;
    }

    HashMap getCacheColumns() {
        return cacheColumns;
    }

    void setCacheColumns(CacheColumn[] cacheColumns) {
        this.cacheColumns = new HashMap<String, CacheColumn>();
        for( int i = 0; i < cacheColumns.length ; i++ ){
            this.cacheColumns.put(cacheColumns[i].getName(), cacheColumns[i]);
            this.cacheColumns.put(""+i, cacheColumns[i]);
        }
    }

}
