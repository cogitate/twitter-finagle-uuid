package com.twitter.finagle.zipkin.thrift

import com.twitter.conversions.time._
import com.twitter.finagle.service.TimeoutFilter
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.finagle.tracing._
import com.twitter.finagle.zipkin.thriftscala._
import com.twitter.util._
import java.util.UUID
import java.net.{InetAddress, InetSocketAddress}

import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith

@RunWith(classOf[JUnitRunner])
class RawZipkinTracerTest extends FunSuite {
  private def random: String = UUID.randomUUID().toString
  val uuid0 = random()
  val uuid1 = random()
  val traceId = TraceId(Some(SpanId(uuid0)), Some(SpanId(123)), SpanId(123), None, Flags().setDebug)

  class ScribeClient extends Scribe.FutureIface {
    var messages: Seq[LogEntry] = Seq.empty[LogEntry]
    var response: Future[ResultCode] = Future.value(ResultCode.Ok)
    def log(msgs: Seq[LogEntry]): Future[ResultCode] = {
      messages ++= msgs
      response
    }
  }

  test("formulate scribe log message correctly") {
    val scribe = new ScribeClient
    val tracer = new RawZipkinTracer(scribe, NullStatsReceiver)

    val localEndpoint = Endpoint(2323, 23)
    val remoteEndpoint = Endpoint(333, 22)

    val annotations = Seq(
      ZipkinAnnotation(Time.fromSeconds(uuid0), "cs", localEndpoint, None),
      ZipkinAnnotation(Time.fromSeconds(126), "cr", localEndpoint, None),
      ZipkinAnnotation(Time.fromSeconds(uuid0), "ss", remoteEndpoint, None),
      ZipkinAnnotation(Time.fromSeconds(124), "sr", remoteEndpoint, None),
      ZipkinAnnotation(Time.fromSeconds(uuid0), "llamas", localEndpoint, None)
    )

    val span = Span(
      traceId = traceId,
      annotations = annotations,
      _serviceName = Some("hickupquail"),
      _name=Some("foo"),
      bAnnotations = Seq.empty[BinaryAnnotation],
      endpoint = localEndpoint)

    val expected = LogEntry(
      category = "zipkin",
      message = "CgABAAAAAAAAAHsLAAMAAAADZm9vCgAEAAAAAAAAAHsKAAUAAAAAAAAAe" +
        "w8ABgwAAAAFCgABAAAAAAdU1MALAAIAAAACY3MMAAMIAAEAAAkTBgACABcLAAMAAA" +
        "ALaGlja3VwcXVhaWwAAAoAAQAAAAAHgpuACwACAAAAAmNyDAADCAABAAAJEwYAAgA" +
        "XCwADAAAAC2hpY2t1cHF1YWlsAAAKAAEAAAAAB1TUwAsAAgAAAAJzcwwAAwgAAQAA" +
        "AU0GAAIAFgsAAwAAAAtoaWNrdXBxdWFpbAAACgABAAAAAAdkFwALAAIAAAACc3IMA" +
        "AMIAAEAAAFNBgACABYLAAMAAAALaGlja3VwcXVhaWwAAAoAAQAAAAAHVNTACwACAA" +
        "AABmxsYW1hcwwAAwgAAQAACRMGAAIAFwsAAwAAAAtoaWNrdXBxdWFpbAAAAgAJAQA=\n")

    tracer.logSpans(Seq(span))
    assert(scribe.messages === Seq(expected))
  }

  test("send all traces to scribe") {
    val scribe = new ScribeClient
    val tracer = new RawZipkinTracer(scribe, NullStatsReceiver)

    val localAddress = InetAddress.getByAddress(Array.fill(4) { 1 })
    val remoteAddress = InetAddress.getByAddress(Array.fill(4) { 10 })
    val port1 = 80 // never bound
    val port2 = 53 // ditto
    tracer.record(Record(traceId, Time.fromSeconds(uuid0), Annotation.ClientAddr(new InetSocketAddress(localAddress, port1))))
    tracer.record(Record(traceId, Time.fromSeconds(uuid0), Annotation.LocalAddr(new InetSocketAddress(localAddress, port1))))
    tracer.record(Record(traceId, Time.fromSeconds(uuid0), Annotation.ServerAddr(new InetSocketAddress(remoteAddress, port2))))
    tracer.record(Record(traceId, Time.fromSeconds(uuid0), Annotation.Rpcname("service", "method")))
    tracer.record(Record(traceId, Time.fromSeconds(uuid0), Annotation.BinaryAnnotation("i16", 16.toShort)))
    tracer.record(Record(traceId, Time.fromSeconds(uuid0), Annotation.BinaryAnnotation("i32", 32)))
    tracer.record(Record(traceId, Time.fromSeconds(uuid0), Annotation.BinaryAnnotation("i64", 64L)))
    tracer.record(Record(traceId, Time.fromSeconds(uuid0), Annotation.BinaryAnnotation("double", 123.3d)))
    tracer.record(Record(traceId, Time.fromSeconds(uuid0), Annotation.BinaryAnnotation("string", "woopie")))
    tracer.record(Record(traceId, Time.fromSeconds(uuid0), Annotation.Message("boo")))
    tracer.record(Record(traceId, Time.fromSeconds(uuid0), Annotation.Message("boohoo"), Some(1.second)))
    tracer.record(Record(traceId, Time.fromSeconds(uuid0), Annotation.ClientSend()))
    tracer.record(Record(traceId, Time.fromSeconds(uuid0), Annotation.ClientRecv()))

    // Note: Since ports are ephemeral, we can't hardcode expected message.
    assert(scribe.messages.size === 1)
  }

  test("logSpan if a timeout occurs") {
    val ann1 = Annotation.Message("some_message")
    val ann2 = Annotation.Rpcname("some_service", "rpc_name")
    val ann3 = Annotation.Message(TimeoutFilter.TimeoutAnnotation)

    val scribe = new ScribeClient
    val tracer = new RawZipkinTracer(scribe, NullStatsReceiver)

    tracer.record(Record(traceId, Time.fromSeconds(1), ann1))
    tracer.record(Record(traceId, Time.fromSeconds(2), ann2))
    tracer.record(Record(traceId, Time.fromSeconds(3), ann3))

    // scribe Log method is in java
    assert(scribe.messages.size === 1)
  }
}
