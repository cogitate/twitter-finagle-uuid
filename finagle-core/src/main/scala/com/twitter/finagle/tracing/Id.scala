package com.twitter.finagle.tracing

import com.twitter.finagle.util.ByteArrays
import com.twitter.util.{Try, Return, Throw}
import com.twitter.util.NonFatal

/**
 * Defines trace identifiers.  Span IDs name a particular (unique)
 * span, while TraceIds contain a span ID as well as context (parentId
 * and traceId).
 */

final class SpanId(val self: String) extends Proxy {
  override def toString: String = self
}

object SpanId {
  def apply(spanId: String): SpanId = new SpanId(spanId)

  def fromString(spanId: String): Option[SpanId] ={
   try {
     Some(SpanId(spanId))
   } catch {
     case NonFatal(_) => None
   }
  }

  override def toString: String = SpanId.toString
}

object TraceId {
  /**
   * Creates a TraceId with no flags set. See case class for more info.
   */
  def apply(
    traceId: Option[SpanId],
    parentId: Option[SpanId],
    spanId: SpanId,
    sampled: Option[Boolean]
  ): TraceId =
    TraceId(traceId, parentId, spanId, sampled, Flags())

  /**
   * Serialize a TraceId into an array of bytes.
   */
  def serialize(traceId: TraceId): Array[Byte] = {
    val flags = traceId._sampled match {
      case None =>
        traceId.flags
      case Some(true) =>
        traceId.flags.setFlag(Flags.SamplingKnown | Flags.Sampled)
      case Some(false) =>
        traceId.flags.setFlag(Flags.SamplingKnown)
    }
    val bytes =  new Array[Byte](56)
    ByteArrays.put128be(bytes, 0, traceId.spanId.toString)
    ByteArrays.put128be(bytes, 16, traceId.parentId.toString)
    ByteArrays.put128be(bytes, 32, traceId.traceId.toString)
    ByteArrays.put128be(bytes, 48, flags.toString)
    bytes
  }

  /**
   * Deserialize a TraceId from an array of bytes.
   */
  def deserialize(bytes: Array[Byte]): Try[TraceId] = {
    if (bytes.length != 56) {
      Throw(new IllegalArgumentException("Expected 56 bytes"))
    } else {
      val span64 = ByteArrays.get128be(bytes, 0)
      val parent64 = ByteArrays.get128be(bytes, 16)
      val trace64 = ByteArrays.get128be(bytes, 32)
      val flags64 = ByteArrays.get128be(bytes, 48)

      val flags = Flags(flags64.toLong)
      val sampled = if (flags.isFlagSet(Flags.SamplingKnown)) {
        Some(flags.isFlagSet(Flags.Sampled))
      } else None

      val traceId = TraceId(
        if (trace64 == parent64) None else Some(SpanId(trace64)),
        if (parent64 == span64) None else Some(SpanId(parent64)),
        SpanId(span64),
        sampled,
        flags)
      Return(traceId)
    }
  }
}

/**
 * A trace id represents one particular trace for one request.
 * @param _traceId The id for this request.
 * @param _parentId The id for the request one step up the service stack.
 * @param spanId The id for this particular request
 * @param _sampled Should we sample this request or not? True means sample, false means don't, none means we defer
 *                decision to someone further down in the stack.
 * @param flags Flags relevant to this request. Could be things like debug mode on/off. The sampled flag could eventually
 *              be moved in here.
 */
final case class TraceId(
  _traceId: Option[SpanId],
  _parentId: Option[SpanId],
  spanId: SpanId,
  _sampled: Option[Boolean],
  flags: Flags)
{
  def traceId: SpanId = _traceId match {
    case None => parentId
    case Some(id) => id
  }

  def parentId: SpanId = _parentId match {
    case None => spanId
    case Some(id) => id
  }

  // debug flag overrides sampled to be true
  lazy val sampled = if (flags.isDebug) Some(true) else _sampled

  private[TraceId] def ids = (traceId, parentId, spanId)

  override def equals(other: Any) = other match {
    case other: TraceId => this.ids equals other.ids
    case _ => false
  }

  override def hashCode(): Int =
    ids.hashCode()

  override def toString =
    "%s.%s<:%s".format(traceId, spanId, parentId)
}
