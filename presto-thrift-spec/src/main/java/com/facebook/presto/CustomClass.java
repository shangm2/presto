package com.facebook.presto;

import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import java.util.Locale;

@ThriftStruct
public class CustomClass
{
    @ThriftField(1)
    public Duration duration;

    @ThriftField(2)
    public DataSize dataSize;

    @ThriftField(3)
    public Locale locale;
}