package com.exasol;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DummyExaIterator implements ExaIterator {

    private List<List<Object>> emittedRows = new ArrayList<>();

    public List<List<Object>> getEmittedRows() {
        return emittedRows;
    }

    @Override
    public void emit(Object... objects) throws ExaIterationException, ExaDataTypeException {
        emittedRows.add(Arrays.asList(objects.clone()));
    }

    @Override
    public long size() throws ExaIterationException {
        return 0;
    }

    @Override
    public boolean next() throws ExaIterationException {
        return false;
    }

    @Override
    public void reset() throws ExaIterationException {

    }

    @Override
    public Integer getInteger(int i) throws ExaIterationException, ExaDataTypeException {
        return null;
    }

    @Override
    public Integer getInteger(String s) throws ExaIterationException, ExaDataTypeException {
        return null;
    }

    @Override
    public Long getLong(int i) throws ExaIterationException, ExaDataTypeException {
        return null;
    }

    @Override
    public Long getLong(String s) throws ExaIterationException, ExaDataTypeException {
        return null;
    }

    @Override
    public BigDecimal getBigDecimal(int i) throws ExaIterationException, ExaDataTypeException {
        return null;
    }

    @Override
    public BigDecimal getBigDecimal(String s) throws ExaIterationException, ExaDataTypeException {
        return null;
    }

    @Override
    public Double getDouble(int i) throws ExaIterationException, ExaDataTypeException {
        return null;
    }

    @Override
    public Double getDouble(String s) throws ExaIterationException, ExaDataTypeException {
        return null;
    }

    @Override
    public String getString(int i) throws ExaIterationException, ExaDataTypeException {
        return null;
    }

    @Override
    public String getString(String s) throws ExaIterationException, ExaDataTypeException {
        return null;
    }

    @Override
    public Boolean getBoolean(int i) throws ExaIterationException, ExaDataTypeException {
        return null;
    }

    @Override
    public Boolean getBoolean(String s) throws ExaIterationException, ExaDataTypeException {
        return null;
    }

    @Override
    public Date getDate(int i) throws ExaIterationException, ExaDataTypeException {
        return null;
    }

    @Override
    public Date getDate(String s) throws ExaIterationException, ExaDataTypeException {
        return null;
    }

    @Override
    public Timestamp getTimestamp(int i) throws ExaIterationException, ExaDataTypeException {
        return null;
    }

    @Override
    public Timestamp getTimestamp(String s) throws ExaIterationException, ExaDataTypeException {
        return null;
    }

    @Override
    public Object getObject(int i) throws ExaIterationException, ExaDataTypeException {
        return null;
    }

    @Override
    public Object getObject(String s) throws ExaIterationException, ExaDataTypeException {
        return null;
    }
}
