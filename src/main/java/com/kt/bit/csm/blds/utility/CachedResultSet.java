package com.kt.bit.csm.blds.utility;

import java.io.Serializable;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.ListIterator;

/**
 * Created with IntelliJ IDEA.
 * User: toto
 * Date: 14. 6. 20
 * Time: 오전 11:29
 * To change this template use File | Settings | File Templates.
 */
public class CachedResultSet implements Serializable {
    private ArrayList<CacheRow> cacheRows;
    private ListIterator iterator;
    private CacheRow currentCacheRow;

    /**
     *
     */
    private static final long serialVersionUID = 2815450470746294066L;


    public CachedResultSet(){
        this.cacheRows = new ArrayList<CacheRow>();
    }

    public CacheRow getCurrentCacheRow() {
        return currentCacheRow;
    }

    public void addRow(CacheColumn[] cacheColumns){
        cacheRows.add(new CacheRow(cacheColumns));
    }

    public void addRow(CacheColumn[] cacheColumns, int rowIndex){
        cacheRows.add(new CacheRow(cacheColumns, rowIndex));
    }


    public boolean next(){
        if(iterator == null)
            iterator = cacheRows.listIterator();

        if(iterator.hasNext()){
            currentCacheRow = (CacheRow)iterator.next();
            return true;
        }
        else
            return false;
    }

    public String getString(int idx) throws Exception{
        Object value = currentCacheRow.getColumn(idx).getValue();
        if( value instanceof String)
            return (String)value;
        else throw new Exception("type mismatch in cachedResultSet");
    }

    public String getString(String name) throws Exception{
        Object value = currentCacheRow.getColumn(name).getValue();
        if( value instanceof String)
            return (String)value;
        else throw new Exception("type mismatch in cachedResultSet");
    }

    public int getInt(int idx) throws Exception{
        Object value = currentCacheRow.getColumn(idx).getValue();
        if( value instanceof Integer)
            return ((Integer)value).intValue();
        else throw new Exception("type mismatch in cachedResultSet");
    }

    public int getInt(String name) throws Exception{
        Object value = currentCacheRow.getColumn(name).getValue();
        if( value instanceof Integer)
            return ((Integer)value).intValue();
        else throw new Exception("type mismatch in cachedResultSet");
    }

    public long getLong(int idx) throws Exception{
        Object value = currentCacheRow.getColumn(idx).getValue();
        if( value instanceof Long)
            return ((Long)value).longValue();
        else throw new Exception("type mismatch in cachedResultSet");
    }

    public long getLong(String name) throws Exception{
        Object value = currentCacheRow.getColumn(name).getValue();
        if( value instanceof Long)
            return ((Long)value).longValue();
        else throw new Exception("type mismatch in cachedResultSet");
    }

    public Date getDate(int idx) throws Exception{
        Object value = currentCacheRow.getColumn(idx).getValue();
        if( value instanceof Date)
            return (Date)value;
        else throw new Exception("type mismatch in cachedResultSet");
    }

    public Date getDate(String name) throws Exception{
        Object value = currentCacheRow.getColumn(name).getValue();
        if( value instanceof Date)
            return (Date)value;
        else throw new Exception("type mismatch in cachedResultSet");
    }

    public Timestamp getTimestamp(int idx) throws Exception {
        Object value = currentCacheRow.getColumn(idx).getValue();
        if( value instanceof Timestamp)
            return (Timestamp)value;
        else throw new Exception("type mismatch in cachedResultSet");
    }

    public Timestamp getTimestamp(String name) throws Exception {
        Object value = currentCacheRow.getColumn(name).getValue();
        if( value instanceof Timestamp)
            return (Timestamp)value;
        else throw new Exception("type mismatch in cachedResultSet");
    }

    public byte[] getBytes(String string) throws SQLException {
        return null;
    }

    public Timestamp getObject(int i) throws SQLException {
        return null;
    }

    public Timestamp getObject(String name) throws SQLException {
        return null;
    }

//    public ResultSetMetaData getMetaData() throws SQLException {
//        return rs.getMetaData();
//    }

    public boolean last(){
        return !iterator.hasNext();
    }

    public int getRowSize(){
        return currentCacheRow.getRowIndex();
    }
}

