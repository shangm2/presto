package com.facebook.presto.server.thrift;

import com.facebook.drift.codec.ThriftCodec;
import com.facebook.drift.codec.ThriftCodecManager;
import com.facebook.drift.protocol.bytebuffer.BufferPool;
import com.facebook.presto.execution.Location;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.split.RemoteSplit;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.testng.AssertJUnit.assertEquals;

public class TestThriftSerde
{
    @Test
    public void testThriftSerde()
            throws Exception
    {
        ThriftCodecManager codecManager = new ThriftCodecManager();
        ThriftCodec<RemoteSplit> thriftCodec = codecManager.getCodec(RemoteSplit.class);
        BufferPool bufferPool = new BufferPool(8, 1024);

        RemoteSplit expectedSplit = new RemoteSplit(new Location("https://www.internalfb.com/intern/scuba/query/?dataset=strobelight_java_asyncprofiler%2Fon_demand&drillstate=%7B%22dimensions%22%3A%5B%5D%2C%22param_dimensions%22%3A%5B%7B%22anchor%22%3A%220%22%2C%22param%22%3A%220%22%2C%22op%22%3A%22all%22%2C%22dim%22%3A%22stack%22%7D%5D%2C%22constraints%22%3A%5B%5B%7B%22value%22%3A%5B%22%5B%5C%227775557930475607%5C%22%5D%22%5D%2C%22op%22%3A%22eq%22%2C%22column%22%3A%22run_id%22%7D%2C%7B%22value%22%3A%5B%22%5B%5C%22shangma%5C%22%5D%22%5D%2C%22op%22%3A%22eq%22%2C%22column%22%3A%22run_user%22%7D%2C%7B%22value%22%3A%5B%22%5B%5C%22strobelight_ui%5C%22%5D%22%5D%2C%22op%22%3A%22all%22%2C%22column%22%3A%22sample_tags%22%7D%2C%7B%22value%22%3A%5B%22%5B%5C%22target%3Dtwshared51610.13.pnb1%5C%22%5D%22%5D%2C%22op%22%3A%22all%22%2C%22column%22%3A%22sample_tags%22%7D%2C%7B%22value%22%3A%5B%22%5B%5C%22target_type%3Dhosts%5C%22%5D%22%5D%2C%22op%22%3A%22all%22%2C%22column%22%3A%22sample_tags%22%7D%5D%5D%2C%22top%22%3A100000%2C%22order%22%3A%22hits%22%2C%22end%22%3A%221751917733%22%2C%22start%22%3A%221751910533%22%2C%22metrik_view_params%22%3A%7B%22columns_skip_formatting%22%3A%5B%5D%2C%22width%22%3Anull%2C%22height%22%3Anull%2C%22tableID%22%3A%22strobelight_java_asyncprofiler%2Fon_demand%22%2C%22fitToContent%22%3Atrue%2C%22show_legend_buttons%22%3Atrue%2C%22use_y_axis_hints_as_limits%22%3Atrue%2C%22legend_mode%22%3A%22nongrid%22%2C%22connect_nulls%22%3Atrue%2C%22timezone_offset%22%3A420%2C%22timezone%22%3A%22America%2FLos_Angeles%22%2C%22y_min_hint%22%3A0%7D%7D&view=sandwich&sandwich_state=%7B%22colorMode%22%3A%22self%22%2C%22deltaMode%22%3A%22none%22%2C%22flamegraphsFormatMode%22%3A%22percentage%22%2C%22tableFormatMode%22%3A%22percentage%22%2C%22savedHighlights%22%3A%5B%5D%2C%22search%22%3A%22%22%2C%22valueMode%22%3A%22selection%22%7D&strobelight_view_state=%7B%22focalFrame%22%3A%7B%22frame%22%3A%22%28root%29%22%2C%22group%22%3A%22%22%7D%7D&icicle_state=%7B%22colorMode%22%3A%22self%22%2C%22deltaMode%22%3A%22none%22%2C%22formatMode%22%3A%22percentage%22%2C%22savedHighlights%22%3A%5B%7B%22highlight%22%3A%22tchunkedbinaryprotoco%22%2C%22color%22%3A%22%23ff00a7%22%7D%2C%7B%22highlight%22%3A%22connectorsplit%22%2C%22color%22%3A%22%23e3ff00%22%7D%5D%2C%22valueMode%22%3A%22selection%22%7D"), new TaskId("test", 1, 0, 2, 0));

        int bufferCount = 0;
        for (int i = 0; i < 10; i++) {
            List<ByteBuffer> buffers = new ArrayList<>();
            ThriftCodecUtils.serializeToBufferList(expectedSplit,
                    thriftCodec,
                    bufferPool,
                    buffers::addAll);
            bufferCount = buffers.size();

            ByteBuffer duplicate = buffers.get(0).duplicate();
            StringBuilder sb = new StringBuilder(duplicate.remaining() * 3);
            while (duplicate.hasRemaining()) {
                byte b = duplicate.get();
                sb.append(String.format("%02X", b & 0xFF));
            }

            RemoteSplit split = ThriftCodecUtils.deserializeFromBufferList(buffers, bufferPool, thriftCodec, RemoteSplit.class);

            assertEquals(expectedSplit.getLocation().getLocation(), split.getLocation().getLocation());
            assertEquals(expectedSplit.getRemoteSourceTaskId(), split.getRemoteSourceTaskId());
        }

        assertEquals(bufferCount, bufferPool.getCount());
    }
}
