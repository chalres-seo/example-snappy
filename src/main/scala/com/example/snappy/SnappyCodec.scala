package com.example.snappy

import java.io.IOException
import java.nio.ByteBuffer
import java.util.zip.CRC32

import com.typesafe.scalalogging.LazyLogging
import org.xerial.snappy.Snappy

object SnappyCodec extends LazyLogging {
  val crc32: CRC32 = new CRC32

  def getCrcIntValue(byteBuffer: ByteBuffer): Int = {
    synchronized {
      crc32.reset()
      crc32.update(byteBuffer.array(), byteBuffer.position, byteBuffer.remaining)
      crc32.getValue.toInt
    }
  }

  def getCrcIntValue(byteArray: Array[Byte] ): Int = {
    synchronized {
      crc32.reset()
      crc32.update(byteArray)
      crc32.getValue.toInt
    }
  }

  def getCrcByteValue(byteArray: Array[Byte] ): Array[Byte] = {
    synchronized {
      crc32.reset()
      crc32.update(byteArray)
      BigInt(crc32.getValue.toInt).toByteArray
    }
  }

  @throws[IOException]
  def compress(in: ByteBuffer): ByteBuffer = {
    val out = ByteBuffer.allocate(Snappy.maxCompressedLength(in.remaining) + 4)
    val size = Snappy.compress(in.array, in.position, in.remaining, out.array, 0)
    logger.debug(s"decompressed size: ${in.limit()} byte, compressed size: $size byte")

    val crcValue = this.getCrcIntValue(in)
    out.putInt(size, crcValue)

    out.limit(size + 4)
    out.slice()
    out
  }

  @throws[IOException]
  def decompress(in: ByteBuffer): ByteBuffer = {
    val out = ByteBuffer.allocate(Snappy.uncompressedLength(in.array, in.position, in.remaining - 4))
    val size = Snappy.uncompress(in.array, in.position, in.remaining - 4, out.array, 0)
    logger.debug(s"compressed size: ${in.limit() - 4} byte, decompressed size: $size byte")

    out.limit(size)
    val crcValue = this.getCrcIntValue(out)

    if (in.getInt(in.limit - 4) != crcValue)
      throw new IOException("Checksum failure")
    out
  }

  @throws[IOException]
  def compress(in: Array[Byte]): Array[Byte] = {
    val crc32: CRC32 = new CRC32
    val out = new Array[Byte](Snappy.maxCompressedLength(in.length) + 4)
    val size = Snappy.compress(in, 0, in.length, out, 0)
    logger.debug(s"decompressed size: ${in.length} byte, compressed size: $size byte")
    crc32.reset()
    crc32.update(in)
    
    val crc32ByteArray = BigInt(crc32.getValue.toInt).toByteArray
    for (index <- crc32ByteArray.indices) {
      out(size + index) = crc32ByteArray(index)
    }

    out.slice(0, size + 4)
  }

  @throws[IOException]
  def decompress(in: Array[Byte]): Array[Byte] = {
    val crc32: CRC32 = new CRC32
    val out = new Array[Byte](Snappy.uncompressedLength(in, 0, in.length))
    val size: Int = Snappy.uncompress(in, 0, in.length - 4, out, 0)
    logger.debug(s"compressed size: ${in.length - 4} byte, decompressed size: $size byte")
    
    crc32.reset()
    crc32.update(out)
    
    val beforeCrc32 = BigInt(in.slice(in.length - 4, in.length)).toInt
    
    if (beforeCrc32 != crc32.getValue.toInt) {
      logger.error(s"check sum failure. before crc32: $beforeCrc32, crc32: ${crc32.getValue.toInt}")
      throw new IOException("Checksum failure")
    }

    out
  }
}
