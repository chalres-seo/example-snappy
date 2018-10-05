package com.example.snappy

import java.nio.charset.StandardCharsets

import org.hamcrest.CoreMatchers._
import org.junit.{Assert, Test}

class TestSnappyCompress {
  @Test
  def testSnappyCompressDecompressByteBuffer(): Unit = {
    val beforeSampleString = "byte buffer sample string, byte buffer sample string"

    val compressed = SnappyCodec.compress(StandardCharsets.UTF_8.encode(beforeSampleString))
    val decompressed = SnappyCodec.decompress(compressed)

    val afterSampleString = StandardCharsets.UTF_8.decode(decompressed).toString

    Assert.assertThat(beforeSampleString, is(afterSampleString))
  }

  @Test
  def testSnappyCompressDecompressArrrayBuffer(): Unit = {
    val beforeSampleString = "byte buffer sample string, byte buffer sample string"

    val compressed: Array[Byte] = SnappyCodec.compress(beforeSampleString.getBytes)
    val decompressed: Array[Byte] = SnappyCodec.decompress(compressed)

    val afterSampleString = new String(decompressed)

    Assert.assertThat(beforeSampleString, is(afterSampleString))
  }
}
