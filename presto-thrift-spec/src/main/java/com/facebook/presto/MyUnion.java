package com.facebook.presto;

import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftUnion;
import com.facebook.drift.annotations.ThriftUnionId;

@ThriftUnion
public final class MyUnion
{
    @ThriftField(1)
    public String stringValue;

    @ThriftField(2)
    public Long longValue;

    @ThriftUnionId
    public short id;
}
