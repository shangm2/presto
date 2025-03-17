package com.facebook.presto.experimental.utils;

import com.facebook.presto.execution.buffer.OutputBuffers;
import com.facebook.presto.experimental.auto_gen.ThriftBufferType;

public class ThriftBufferTypeUtils
{
    private ThriftBufferTypeUtils() {}

    public static OutputBuffers.BufferType toBufferType(ThriftBufferType thriftBufferType)
    {
        return OutputBuffers.BufferType.valueOf(thriftBufferType.name());
    }

    public static ThriftBufferType fromBufferType(OutputBuffers.BufferType bufferType)
    {
        return ThriftBufferType.valueOf(bufferType.name());
    }
}
