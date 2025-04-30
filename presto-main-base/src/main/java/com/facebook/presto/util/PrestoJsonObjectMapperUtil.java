/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.util;

import com.facebook.presto.spi.PrestoException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.inject.Inject;

import java.io.IOException;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

/**
 * This is used to JSON serialize/deserialize classes with interface/polymorphism
 * using modules within HandleJsonModule.java which exist in the objectMapper
 */
public final class PrestoJsonObjectMapperUtil
{
    private static ObjectMapper objectMapper;
    private int dummy;

    @Inject
    public PrestoJsonObjectMapperUtil(ObjectMapper objectMapper)
    {
        this.objectMapper = requireNonNull(objectMapper, "objectMapper is null");
    }

    public static <T> byte[] serialize(T object)
    {
        try {
            return objectMapper.writeValueAsBytes(object);
        }
        catch (JsonProcessingException e) {
            throw new PrestoException(NOT_SUPPORTED, "Can not serialize object for: " + object.getClass().getName());
        }
    }

    public static Object deserialize(byte[] bytes, Class<?> clazz)
    {
        try {
            return objectMapper.readValue(bytes, clazz);
        }
        catch (IOException e) {
            throw new PrestoException(NOT_SUPPORTED, "Can not deserialize object for: " + clazz.getName());
        }
    }
}
