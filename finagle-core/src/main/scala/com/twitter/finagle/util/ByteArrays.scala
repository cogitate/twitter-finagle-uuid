package com.twitter.finagle.util

import java.util.UUID

object ByteArrays {
  /**
   * An efficient implementation of adding two Array[Byte] objects togther.
   * About 20x faster than (a ++ b).
   */
  def concat(a: Array[Byte], b: Array[Byte]): Array[Byte] = {
    val res = new Array[Byte](a.length + b.length)
    System.arraycopy(a, 0, res, 0, a.length)
    System.arraycopy(b, 0, res, a.length, b.length)
    res
  }

  /**
   * Writes the sixteen bytes of `traces` into `bytes` in big-endian order, starting
   * at the position `i`.
   */
   def put128be(bytes: Array[Byte], i: Int, trace: String):Array[Byte] = {
    if (i<48) {
        val uuid = UUID.fromString(trace)
        val msb = uuid.getMostSignificantBits
        val lsb = uuid.getLeastSignificantBits
        bytes(i) =  (msb >>> 56).toByte
        bytes(i+1) =  (msb >>> 48).toByte
        bytes(i+2) = (msb >>> 40).toByte
        bytes(i+3) = (msb >>> 32).toByte
        bytes(i+4) = (msb >>> 24).toByte
        bytes(i+5) = (msb >>> 16).toByte
        bytes(i+6) = (msb >>>  8).toByte
        bytes(i+7) = (msb       ).toByte
        bytes(i+8) = (lsb >>> 56).toByte
        bytes(i+9) = (lsb >>> 48).toByte
        bytes(i+10) = (lsb >>> 40).toByte
        bytes(i+11) = (lsb >>> 32).toByte
        bytes(i+12) = (lsb >>> 24).toByte
        bytes(i+13) = (lsb >>> 16).toByte
        bytes(i+14) = (lsb >>>  8).toByte
        bytes(i+15) = (lsb       ).toByte
    } else {
        val l = trace.toLong
        bytes(i) =  (l >>> 56).toByte
        bytes(i+1) =  (l >>> 48).toByte
        bytes(i+2) = (l >>> 40).toByte
        bytes(i+3) = (l >>> 32).toByte
        bytes(i+4) = (l >>> 24).toByte
        bytes(i+5) = (l >>> 16).toByte
        bytes(i+6) = (l >>>  8).toByte
        bytes(i+7) = (l       ).toByte
    }
    bytes
  }

  /**
   * Returns the 16 bytes of `bytes` in big-endian order, starting at `i`.
   */
  def get128be(bytes: Array[Byte], i: Int): String = {
      val ffl = 0xffL
      //these are UUIDs, need a different fetch algorithm
      if (i<48) {
          val msb = (
              (bytes(i) & ffl) << 56 | (bytes(i+1) & ffl) <<  48 |
              (bytes(i+2) & ffl) << 40 | (bytes(i+3) & ffl) <<  32 |
              (bytes(i+4) & ffl) << 24 | (bytes(i+5) & ffl) <<  16 |
              (bytes(i+6) & ffl) << 8 | (bytes(i+7) & ffl)
          )
          val lsb = (
              (bytes(i+8) & ffl) << 56 | (bytes(i+9) & ffl) <<  48 |
              (bytes(i+10) & ffl) << 40 | (bytes(i+11) & ffl) <<  32 |
              (bytes(i+12) & ffl) << 24 | (bytes(i+13) & ffl) <<  16 |
              (bytes(i+14) & ffl) << 8 | (bytes(i+15) & ffl)
          )
          val uuid = new UUID(msb, lsb)
          return uuid.toString
      }
      //this is a Flag, this is a long
      else {
          val flag = (bytes(i) & ffl) << 56 | (bytes(i+1) & ffl) <<  48 |
              (bytes(i+2) & ffl) << 40 | (bytes(i+3) & ffl) <<  32 |
              (bytes(i+4) & ffl) << 24 | (bytes(i+5) & ffl) <<  16 |
              (bytes(i+6) & ffl) << 8 | (bytes(i+7) & ffl)
          return flag.toString
      }
  }


    /**
   * Writes the eight bytes of `l` into `bytes` in big-endian order, starting
   * at the position `i`.
   */
  def put64be(bytes: Array[Byte], i: Int, l: Long) {
    bytes(i) = (l>>56 & 0xff).toByte
    bytes(i+1) = (l>>48 & 0xff).toByte
    bytes(i+2) = (l>>40 & 0xff).toByte
    bytes(i+3) = (l>>32 & 0xff).toByte
    bytes(i+4) = (l>>24 & 0xff).toByte
    bytes(i+5) = (l>>16 & 0xff).toByte
    bytes(i+6) = (l>>8 & 0xff).toByte
    bytes(i+7) = (l & 0xff).toByte
  }

  /**
   * Returns the eight bytes of `bytes` in big-endian order, starting at `i`.
   */
  def get64be(bytes: Array[Byte], i: Int): Long = {
    ((bytes(i) & 0xff).toLong << 56) |
    ((bytes(i+1) & 0xff).toLong << 48) |
    ((bytes(i+2) & 0xff).toLong << 40) |
    ((bytes(i+3) & 0xff).toLong << 32) |
    ((bytes(i+4) & 0xff).toLong << 24) |
    ((bytes(i+5) & 0xff).toLong << 16) |
    ((bytes(i+6) & 0xff).toLong << 8) |
    (bytes(i+7) & 0xff).toLong
  }
}
