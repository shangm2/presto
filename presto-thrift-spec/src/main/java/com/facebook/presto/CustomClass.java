package com.facebook.presto;

import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import java.util.Locale;
import java.util.Optional;

@ThriftStruct
public class CustomClass
{
    @ThriftField(1)
    public Duration duration;

    @ThriftField(2)
    public DataSize dataSize;

    @ThriftField(3)
    public Locale locale;

    @ThriftField(4)
    public MyUnion myUnionField;

    @ThriftField(5)
    public Optional<TableWriteInfo> tableWriteInfo;
}