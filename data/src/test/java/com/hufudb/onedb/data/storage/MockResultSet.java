package com.hufudb.onedb.data.storage;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

public class MockResultSet implements ResultSet {
  final DataSetIterator source;

  public MockResultSet(DataSet source) {
    this.source = source.getIterator();
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    // pass
    return false;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    // pass
    return null;
  }

  @Override
  public boolean absolute(int row) throws SQLException {
    // pass
    return false;
  }

  @Override
  public void afterLast() throws SQLException {
    // pass

  }

  @Override
  public void beforeFirst() throws SQLException {
    // pass

  }

  @Override
  public void cancelRowUpdates() throws SQLException {
    // pass

  }

  @Override
  public void clearWarnings() throws SQLException {
    // pass

  }

  @Override
  public void close() throws SQLException {
    // pass

  }

  @Override
  public void deleteRow() throws SQLException {
    // pass

  }

  @Override
  public int findColumn(String columnLabel) throws SQLException {
    // pass
    return 0;
  }

  @Override
  public boolean first() throws SQLException {
    // pass
    return false;
  }

  @Override
  public Array getArray(int columnIndex) throws SQLException {
    // pass
    return null;
  }

  @Override
  public Array getArray(String columnLabel) throws SQLException {
    // pass
    return null;
  }

  @Override
  public InputStream getAsciiStream(int columnIndex) throws SQLException {
    // pass
    return null;
  }

  @Override
  public InputStream getAsciiStream(String columnLabel) throws SQLException {
    // pass
    return null;
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
    // pass
    return null;
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
    // pass
    return null;
  }

  @Override
  public BigDecimal getBigDecimal(int arg0, int arg1) throws SQLException {
    // pass
    return null;
  }

  @Override
  public BigDecimal getBigDecimal(String arg0, int arg1) throws SQLException {
    // pass
    return null;
  }

  @Override
  public InputStream getBinaryStream(int columnIndex) throws SQLException {
    // pass
    return null;
  }

  @Override
  public InputStream getBinaryStream(String columnLabel) throws SQLException {
    // pass
    return null;
  }

  @Override
  public Blob getBlob(int columnIndex) throws SQLException {
    // pass
    return null;
  }

  @Override
  public Blob getBlob(String columnLabel) throws SQLException {
    // pass
    return null;
  }

  @Override
  public boolean getBoolean(int columnIndex) throws SQLException {
    // pass
    return (Boolean) source.get(columnIndex - 1);
  }

  @Override
  public boolean getBoolean(String columnLabel) throws SQLException {
    // pass
    return false;
  }

  @Override
  public byte getByte(int columnIndex) throws SQLException {
    // pass
    return 0;
  }

  @Override
  public byte getByte(String columnLabel) throws SQLException {
    // pass
    return 0;
  }

  @Override
  public byte[] getBytes(int columnIndex) throws SQLException {
    // pass
    return (byte[]) source.get(columnIndex - 1);
  }

  @Override
  public byte[] getBytes(String columnLabel) throws SQLException {
    // pass
    return null;
  }

  @Override
  public Reader getCharacterStream(int columnIndex) throws SQLException {
    // pass
    return null;
  }

  @Override
  public Reader getCharacterStream(String columnLabel) throws SQLException {
    // pass
    return null;
  }

  @Override
  public Clob getClob(int columnIndex) throws SQLException {
    // pass
    return null;
  }

  @Override
  public Clob getClob(String columnLabel) throws SQLException {
    // pass
    return null;
  }

  @Override
  public int getConcurrency() throws SQLException {
    // pass
    return 0;
  }

  @Override
  public String getCursorName() throws SQLException {
    // pass
    return null;
  }

  @Override
  public Date getDate(int columnIndex) throws SQLException {
    // pass
    return null;
  }

  @Override
  public Date getDate(String columnLabel) throws SQLException {
    // pass
    return null;
  }

  @Override
  public Date getDate(int arg0, Calendar arg1) throws SQLException {
    // pass
    return null;
  }

  @Override
  public Date getDate(String arg0, Calendar arg1) throws SQLException {
    // pass
    return null;
  }

  @Override
  public double getDouble(int columnIndex) throws SQLException {
    // pass
    return (Double) source.get(columnIndex - 1);
  }

  @Override
  public double getDouble(String columnLabel) throws SQLException {
    // pass
    return 0;
  }

  @Override
  public int getFetchDirection() throws SQLException {
    // pass
    return 0;
  }

  @Override
  public int getFetchSize() throws SQLException {
    // pass
    return 0;
  }

  @Override
  public float getFloat(int columnIndex) throws SQLException {
    // pass
    return (Float) source.get(columnIndex - 1);
  }

  @Override
  public float getFloat(String columnLabel) throws SQLException {
    // pass
    return 0;
  }

  @Override
  public int getHoldability() throws SQLException {
    // pass
    return 0;
  }

  @Override
  public int getInt(int columnIndex) throws SQLException {
    // pass
    return (Integer) source.get(columnIndex - 1);
  }

  @Override
  public int getInt(String columnLabel) throws SQLException {
    // pass
    return 0;
  }

  @Override
  public long getLong(int columnIndex) throws SQLException {
    // pass
    return (Long) source.get(columnIndex - 1);
  }

  @Override
  public long getLong(String columnLabel) throws SQLException {
    // pass
    return 0;
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    // pass
    return null;
  }

  @Override
  public Reader getNCharacterStream(int columnIndex) throws SQLException {
    // pass
    return null;
  }

  @Override
  public Reader getNCharacterStream(String columnLabel) throws SQLException {
    // pass
    return null;
  }

  @Override
  public NClob getNClob(int columnIndex) throws SQLException {
    // pass
    return null;
  }

  @Override
  public NClob getNClob(String columnLabel) throws SQLException {
    // pass
    return null;
  }

  @Override
  public String getNString(int columnIndex) throws SQLException {
    // pass
    return null;
  }

  @Override
  public String getNString(String columnLabel) throws SQLException {
    // pass
    return null;
  }

  @Override
  public Object getObject(int columnIndex) throws SQLException {
    // pass
    return source.get(columnIndex - 1);
  }

  @Override
  public Object getObject(String columnLabel) throws SQLException {
    // pass
    return null;
  }

  @Override
  public Object getObject(int arg0, Map<String, Class<?>> arg1) throws SQLException {
    // pass
    return null;
  }

  @Override
  public Object getObject(String arg0, Map<String, Class<?>> arg1) throws SQLException {
    // pass
    return null;
  }

  @Override
  public <T> T getObject(int arg0, Class<T> arg1) throws SQLException {
    // pass
    return null;
  }

  @Override
  public <T> T getObject(String arg0, Class<T> arg1) throws SQLException {
    // pass
    return null;
  }

  @Override
  public Ref getRef(int columnIndex) throws SQLException {
    // pass
    return null;
  }

  @Override
  public Ref getRef(String columnLabel) throws SQLException {
    // pass
    return null;
  }

  @Override
  public int getRow() throws SQLException {
    // pass
    return 0;
  }

  @Override
  public RowId getRowId(int columnIndex) throws SQLException {
    // pass
    return null;
  }

  @Override
  public RowId getRowId(String columnLabel) throws SQLException {
    // pass
    return null;
  }

  @Override
  public SQLXML getSQLXML(int columnIndex) throws SQLException {
    // pass
    return null;
  }

  @Override
  public SQLXML getSQLXML(String columnLabel) throws SQLException {
    // pass
    return null;
  }

  @Override
  public short getShort(int columnIndex) throws SQLException {
    // pass
    return 0;
  }

  @Override
  public short getShort(String columnLabel) throws SQLException {
    // pass
    return 0;
  }

  @Override
  public Statement getStatement() throws SQLException {
    // pass
    return null;
  }

  @Override
  public String getString(int columnIndex) throws SQLException {
    // pass
    return (String) source.get(columnIndex - 1);
  }

  @Override
  public String getString(String columnLabel) throws SQLException {
    // pass
    return null;
  }

  @Override
  public Time getTime(int columnIndex) throws SQLException {
    // pass
    return null;
  }

  @Override
  public Time getTime(String columnLabel) throws SQLException {
    // pass
    return null;
  }

  @Override
  public Time getTime(int arg0, Calendar arg1) throws SQLException {
    // pass
    return null;
  }

  @Override
  public Time getTime(String arg0, Calendar arg1) throws SQLException {
    // pass
    return null;
  }

  @Override
  public Timestamp getTimestamp(int columnIndex) throws SQLException {
    // pass
    return null;
  }

  @Override
  public Timestamp getTimestamp(String columnLabel) throws SQLException {
    // pass
    return null;
  }

  @Override
  public Timestamp getTimestamp(int arg0, Calendar arg1) throws SQLException {
    // pass
    return null;
  }

  @Override
  public Timestamp getTimestamp(String arg0, Calendar arg1) throws SQLException {
    // pass
    return null;
  }

  @Override
  public int getType() throws SQLException {
    // pass
    return 0;
  }

  @Override
  public URL getURL(int columnIndex) throws SQLException {
    // pass
    return null;
  }

  @Override
  public URL getURL(String columnLabel) throws SQLException {
    // pass
    return null;
  }

  @Override
  public InputStream getUnicodeStream(int arg0) throws SQLException {
    // pass
    return null;
  }

  @Override
  public InputStream getUnicodeStream(String arg0) throws SQLException {
    // pass
    return null;
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    // pass
    return null;
  }

  @Override
  public void insertRow() throws SQLException {
    // pass

  }

  @Override
  public boolean isAfterLast() throws SQLException {
    // pass
    return false;
  }

  @Override
  public boolean isBeforeFirst() throws SQLException {
    // pass
    return false;
  }

  @Override
  public boolean isClosed() throws SQLException {
    // pass
    return false;
  }

  @Override
  public boolean isFirst() throws SQLException {
    // pass
    return false;
  }

  @Override
  public boolean isLast() throws SQLException {
    // pass
    return false;
  }

  @Override
  public boolean last() throws SQLException {
    // pass
    return false;
  }

  @Override
  public void moveToCurrentRow() throws SQLException {
    // pass

  }

  @Override
  public void moveToInsertRow() throws SQLException {
    // pass

  }

  @Override
  public boolean next() throws SQLException {
    return source.next();
  }

  @Override
  public boolean previous() throws SQLException {
    // pass
    return false;
  }

  @Override
  public void refreshRow() throws SQLException {
    // pass

  }

  @Override
  public boolean relative(int rows) throws SQLException {
    // pass
    return false;
  }

  @Override
  public boolean rowDeleted() throws SQLException {
    // pass
    return false;
  }

  @Override
  public boolean rowInserted() throws SQLException {
    // pass
    return false;
  }

  @Override
  public boolean rowUpdated() throws SQLException {
    // pass
    return false;
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    // pass

  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    // pass

  }

  @Override
  public void updateArray(int arg0, Array arg1) throws SQLException {
    // pass

  }

  @Override
  public void updateArray(String arg0, Array arg1) throws SQLException {
    // pass

  }

  @Override
  public void updateAsciiStream(int arg0, InputStream arg1) throws SQLException {
    // pass

  }

  @Override
  public void updateAsciiStream(String arg0, InputStream arg1) throws SQLException {
    // pass

  }

  @Override
  public void updateAsciiStream(int arg0, InputStream arg1, int arg2) throws SQLException {
    // pass

  }

  @Override
  public void updateAsciiStream(String arg0, InputStream arg1, int arg2) throws SQLException {
    // pass

  }

  @Override
  public void updateAsciiStream(int arg0, InputStream arg1, long arg2) throws SQLException {
    // pass

  }

  @Override
  public void updateAsciiStream(String arg0, InputStream arg1, long arg2) throws SQLException {
    // pass

  }

  @Override
  public void updateBigDecimal(int arg0, BigDecimal arg1) throws SQLException {
    // pass

  }

  @Override
  public void updateBigDecimal(String arg0, BigDecimal arg1) throws SQLException {
    // pass

  }

  @Override
  public void updateBinaryStream(int arg0, InputStream arg1) throws SQLException {
    // pass

  }

  @Override
  public void updateBinaryStream(String arg0, InputStream arg1) throws SQLException {
    // pass

  }

  @Override
  public void updateBinaryStream(int arg0, InputStream arg1, int arg2) throws SQLException {
    // pass

  }

  @Override
  public void updateBinaryStream(String arg0, InputStream arg1, int arg2) throws SQLException {
    // pass

  }

  @Override
  public void updateBinaryStream(int arg0, InputStream arg1, long arg2) throws SQLException {
    // pass

  }

  @Override
  public void updateBinaryStream(String arg0, InputStream arg1, long arg2) throws SQLException {
    // pass

  }

  @Override
  public void updateBlob(int arg0, Blob arg1) throws SQLException {
    // pass

  }

  @Override
  public void updateBlob(String arg0, Blob arg1) throws SQLException {
    // pass

  }

  @Override
  public void updateBlob(int arg0, InputStream arg1) throws SQLException {
    // pass

  }

  @Override
  public void updateBlob(String arg0, InputStream arg1) throws SQLException {
    // pass

  }

  @Override
  public void updateBlob(int arg0, InputStream arg1, long arg2) throws SQLException {
    // pass

  }

  @Override
  public void updateBlob(String arg0, InputStream arg1, long arg2) throws SQLException {
    // pass

  }

  @Override
  public void updateBoolean(int arg0, boolean arg1) throws SQLException {
    // pass

  }

  @Override
  public void updateBoolean(String arg0, boolean arg1) throws SQLException {
    // pass

  }

  @Override
  public void updateByte(int arg0, byte arg1) throws SQLException {
    // pass

  }

  @Override
  public void updateByte(String arg0, byte arg1) throws SQLException {
    // pass

  }

  @Override
  public void updateBytes(int arg0, byte[] arg1) throws SQLException {
    // pass

  }

  @Override
  public void updateBytes(String arg0, byte[] arg1) throws SQLException {
    // pass

  }

  @Override
  public void updateCharacterStream(int arg0, Reader arg1) throws SQLException {
    // pass

  }

  @Override
  public void updateCharacterStream(String arg0, Reader arg1) throws SQLException {
    // pass

  }

  @Override
  public void updateCharacterStream(int arg0, Reader arg1, int arg2) throws SQLException {
    // pass

  }

  @Override
  public void updateCharacterStream(String arg0, Reader arg1, int arg2) throws SQLException {
    // pass

  }

  @Override
  public void updateCharacterStream(int arg0, Reader arg1, long arg2) throws SQLException {
    // pass

  }

  @Override
  public void updateCharacterStream(String arg0, Reader arg1, long arg2) throws SQLException {
    // pass

  }

  @Override
  public void updateClob(int arg0, Clob arg1) throws SQLException {
    // pass

  }

  @Override
  public void updateClob(String arg0, Clob arg1) throws SQLException {
    // pass

  }

  @Override
  public void updateClob(int arg0, Reader arg1) throws SQLException {
    // pass

  }

  @Override
  public void updateClob(String arg0, Reader arg1) throws SQLException {
    // pass

  }

  @Override
  public void updateClob(int arg0, Reader arg1, long arg2) throws SQLException {
    // pass

  }

  @Override
  public void updateClob(String arg0, Reader arg1, long arg2) throws SQLException {
    // pass

  }

  @Override
  public void updateDate(int arg0, Date arg1) throws SQLException {
    // pass

  }

  @Override
  public void updateDate(String arg0, Date arg1) throws SQLException {
    // pass

  }

  @Override
  public void updateDouble(int arg0, double arg1) throws SQLException {
    // pass

  }

  @Override
  public void updateDouble(String arg0, double arg1) throws SQLException {
    // pass

  }

  @Override
  public void updateFloat(int arg0, float arg1) throws SQLException {
    // pass

  }

  @Override
  public void updateFloat(String arg0, float arg1) throws SQLException {
    // pass

  }

  @Override
  public void updateInt(int arg0, int arg1) throws SQLException {
    // pass

  }

  @Override
  public void updateInt(String arg0, int arg1) throws SQLException {
    // pass

  }

  @Override
  public void updateLong(int arg0, long arg1) throws SQLException {
    // pass

  }

  @Override
  public void updateLong(String arg0, long arg1) throws SQLException {
    // pass

  }

  @Override
  public void updateNCharacterStream(int arg0, Reader arg1) throws SQLException {
    // pass

  }

  @Override
  public void updateNCharacterStream(String arg0, Reader arg1) throws SQLException {
    // pass

  }

  @Override
  public void updateNCharacterStream(int arg0, Reader arg1, long arg2) throws SQLException {
    // pass

  }

  @Override
  public void updateNCharacterStream(String arg0, Reader arg1, long arg2) throws SQLException {
    // pass

  }

  @Override
  public void updateNClob(int arg0, NClob arg1) throws SQLException {
    // pass

  }

  @Override
  public void updateNClob(String arg0, NClob arg1) throws SQLException {
    // pass

  }

  @Override
  public void updateNClob(int arg0, Reader arg1) throws SQLException {
    // pass

  }

  @Override
  public void updateNClob(String arg0, Reader arg1) throws SQLException {
    // pass

  }

  @Override
  public void updateNClob(int arg0, Reader arg1, long arg2) throws SQLException {
    // pass

  }

  @Override
  public void updateNClob(String arg0, Reader arg1, long arg2) throws SQLException {
    // pass

  }

  @Override
  public void updateNString(int arg0, String arg1) throws SQLException {
    // pass

  }

  @Override
  public void updateNString(String arg0, String arg1) throws SQLException {
    // pass

  }

  @Override
  public void updateNull(int columnIndex) throws SQLException {
    // pass
  }

  @Override
  public void updateNull(String columnLabel) throws SQLException {
    // pass
  }

  @Override
  public void updateObject(int arg0, Object arg1) throws SQLException {
    // pass
  }

  @Override
  public void updateObject(String arg0, Object arg1) throws SQLException {
    // pass
  }

  @Override
  public void updateObject(int arg0, Object arg1, int arg2) throws SQLException {
    // pass
  }

  @Override
  public void updateObject(String arg0, Object arg1, int arg2) throws SQLException {
    // pass
  }

  @Override
  public void updateRef(int arg0, Ref arg1) throws SQLException {
    // pass
  }

  @Override
  public void updateRef(String arg0, Ref arg1) throws SQLException {
    // pass
  }

  @Override
  public void updateRow() throws SQLException {
    // pass
  }

  @Override
  public void updateRowId(int arg0, RowId arg1) throws SQLException {
    // pass
  }

  @Override
  public void updateRowId(String arg0, RowId arg1) throws SQLException {
    // pass
  }

  @Override
  public void updateSQLXML(int arg0, SQLXML arg1) throws SQLException {
    // pass
  }

  @Override
  public void updateSQLXML(String arg0, SQLXML arg1) throws SQLException {
    // pass
  }

  @Override
  public void updateShort(int arg0, short arg1) throws SQLException {
    // pass
  }

  @Override
  public void updateShort(String arg0, short arg1) throws SQLException {
    // pass
  }

  @Override
  public void updateString(int arg0, String arg1) throws SQLException {
    // pass
  }

  @Override
  public void updateString(String arg0, String arg1) throws SQLException {
    // pass
  }

  @Override
  public void updateTime(int arg0, Time arg1) throws SQLException {
    // pass
  }

  @Override
  public void updateTime(String arg0, Time arg1) throws SQLException {
    // pass
  }

  @Override
  public void updateTimestamp(int arg0, Timestamp arg1) throws SQLException {
    // pass
  }

  @Override
  public void updateTimestamp(String arg0, Timestamp arg1) throws SQLException {
    // pass
  }

  @Override
  public boolean wasNull() throws SQLException {
    return false;
  }
}
