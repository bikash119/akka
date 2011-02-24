/**
 * Copyright (C) 2009-2011 Scalable Solutions AB <http://scalablesolutions.se>
 */

package remote

import akka.actor.Actor
import akka.actor.Actor._
import akka.util.{Logging, TestKit, Address}
import akka.util.duration._
import collection.mutable.ListBuffer
import org.scalatest.matchers.ShouldMatchers
import java.lang.String
import akka.remote.netty.NettyRemoteSupport
import org.scalatest.{BeforeAndAfterAll, WordSpec}
import akka.remote.protocol.RemoteProtocol.RemoteMessageProtocol
import akka.remote.protocol.RemoteProtocol.MetadataEntryProtocol
import akka.remote.{MessageSerializer, Pipeline}
import com.google.protobuf.ByteString

/**
 * Test for Ticket #597, tests for Pipeline
 */
class PipelineTest extends WordSpec with ShouldMatchers with BeforeAndAfterAll with TestKit with Logging {
  val remote = Actor.remote
  val host = "localhost"
  val port = 25520
  val timeoutTestActor = 50
  val actorName = "test-name"
  val anotherName = "under another name"
  val echoName = "echo"

  def OptimizeLocal = false

  var optimizeLocal_? = remote.asInstanceOf[NettyRemoteSupport].optimizeLocalScoped_?

  override def beforeAll {
    setTestActorTimeout(timeoutTestActor seconds)
    remote.start(host, port)
    Thread.sleep(2000)
    if (!OptimizeLocal)
      remote.asInstanceOf[NettyRemoteSupport].optimizeLocal.set(false) //Can't run the test if we're eliminating all remote calls
    remote.register(actorName, testActor)
    remote.register(echoName, actorOf(new EchoActor).start)
  }

  override def afterAll {
    if (!OptimizeLocal)
      remote.asInstanceOf[NettyRemoteSupport].optimizeLocal.set(optimizeLocal_?) //Reset optimizelocal after all tests
    remote.shutdown
    Actor.registry.shutdownAll
    stopTestActor
  }

  "A registered client send filter" should {
    "get the request passed through it with a PassThrough filter" in {
      within(2 seconds) {
        val filter = PassThrough(actorName)
        Pipeline.registerClientFilters(Address(host, port), filter.filter)
        filter.interceptedMessages should have size (0)
        remote.actorFor(actorName, host, port) ! "test"
        expectMsg("test")
        filter.interceptedMessages should have size (1)
        filter.interceptedMessages map {
          protocol => protocol.getActorInfo.getId should be(actorName)
        }
      }
    }
  }
  "A registered client send filter" should {
    val filter = FilterByName(actorName)
    val remoteActor = remote.actorFor(actorName, host, port)
    "get the request passed through it and exclude the message with FilterByName" in {
      within(2 seconds) {
        Pipeline.registerClientFilters(Address(host, port), filter.filter)
        filter.interceptedMessages should have size (0)
        remoteActor ! "test"
        expectNoMsg
        filter.interceptedMessages should have size (1)
        filter.interceptedMessages map {
          protocol => protocol.getActorInfo.getId should be(actorName)
        }
      }
    }
    "not be active after another filter is set" in {
      within(2 seconds) {
        Pipeline.registerClientFilters(Address(host, port), Pipeline.identity)
        remote.actorFor(actorName, host, port) ! "test"
        expectMsg("test")
        filter.interceptedMessages should have size (1)
      }
    }
    "be active after the filter is set again" in {
      within(2 seconds) {
        Pipeline.registerClientFilters(Address(host, port), filter.filter)
        remote.actorFor(actorName, host, port) ! "test"
        expectNoMsg
        filter.interceptedMessages should have size (2)
      }
    }
    "not be active after filters are unregistered" in {
      within(2 seconds) {
        Pipeline.unregisterClientFilters(Address(host, port))
        remote.actorFor(actorName, host, port) ! "test"
        expectMsg("test")
        filter.interceptedMessages should have size (2)
      }
    }

    "get the request passed through it without FilterByName" in {
      within(2 seconds) {
        val anotherFilter = FilterByName(anotherName)
        Pipeline.registerClientFilters(Address(host, port), anotherFilter.filter)
        anotherFilter.interceptedMessages should have size (0)
        remote.actorFor(actorName, host, port) ! "test"
        expectMsg("test")
        anotherFilter.interceptedMessages should have size (0)
      }
    }

    "get the request passed through it and modify the message with Modify" in {
      val filter = Modify(echoName)
      Pipeline.registerClientFilters(Address(host, port), filter.filter)
      filter.interceptedMessages should have size (0)
      val reply = remote.actorFor(echoName, host, port) !! "test"
      reply match {
        case Some(msg) => reply.get should be("changed the message in the pipeline")
        case _ => fail("incorrect reply")
      }
      filter.interceptedMessages should have size (1)
      filter.interceptedMessages map {
        protocol => protocol.getActorInfo.getId should be(echoName)
      }
    }
    "get the request passed through it and add header to the message with AddMetaData" in {
      val addMetaData = AddMetaData(echoName, "key", "somedata")
      val getMetaData = GetMetaData(echoName)

      Pipeline.registerClientFilters(Address(host, port), addMetaData.filter)
      Pipeline.registerServerFilters(Address(host, port),Pipeline.identity,getMetaData.filter)
      addMetaData.interceptedMessages should have size (0)
      getMetaData.interceptedMessages should have size (0)
      val reply = remote.actorFor(echoName, host, port) !! "test"
      reply match {
        case Some(msg) => reply.get should be("test")
        case _ => fail("incorrect reply")
      }
      addMetaData.interceptedMessages should have size (1)
      getMetaData.interceptedMessages should have size (1)
      addMetaData.interceptedMessages map {
        protocol => protocol.getActorInfo.getId should be(echoName)
      }
      val header = getMetaData.header
      header._1 should be("key")
      header._2 should be("somedata")
    }

  }
  "A registered client receive filter" should {
    "get the reply passed through it" in {
      val filter = PassThrough(echoName)
      Pipeline.registerClientFilters(Address(host, port), Pipeline.identity, filter.filter)
      filter.interceptedMessages should have size (0)
      val reply = remote.actorFor(echoName, host, port) !! "test"
      reply match {
        case Some(msg) => reply.get should be("test")
        case _ => fail("incorrect reply")
      }
      filter.interceptedMessages should have size (1)
      filter.interceptedMessages map {
        protocol => protocol.getActorInfo.getId should be(echoName)
      }
    }
  }

  "A registered server receive filter" should {
    val filter = PassThrough(echoName)
    "get the request passed through it" in {
      Pipeline.registerServerFilters(Address(host, port), Pipeline.identity, filter.filter)
      filter.interceptedMessages should have size (0)
      val reply = remote.actorFor(echoName, host, port) !! "test"
      reply match {
        case Some(msg) => reply.get should be("test")
        case _ => fail("incorrect reply")
      }
      filter.interceptedMessages should have size (1)
      filter.interceptedMessages map {
        protocol => protocol.getActorInfo.getId should be(echoName)
      }
    }
    "get no request passed through it after unregister" in {
      Pipeline.unregisterServerFilters(Address(host, port))
      filter.interceptedMessages should have size (1)
      val reply = remote.actorFor(echoName, host, port) !! "test"
      reply match {
        case Some(msg) => reply.get should be("test")
        case _ => fail("incorrect reply")
      }
      filter.interceptedMessages should have size (1)
    }
  }

  "A registered server send filter" should {
    "get the reply passed through it" in {
      val filter = PassThrough(echoName)
      Pipeline.registerServerFilters(Address(host, port), filter.filter, Pipeline.identity)
      filter.interceptedMessages should have size (0)
      val reply = remote.actorFor(echoName, host, port) !! "test"
      reply match {
        case Some(msg) => reply.get should be("test")
        case _ => fail("incorrect reply")
      }
      filter.interceptedMessages should have size (1)
      filter.interceptedMessages map {
        protocol => protocol.getActorInfo.getId should be(echoName)
      }
    }
  }
}

case class FilterByName(id: String) {
  val messages = ListBuffer[RemoteMessageProtocol]()

  def interceptedMessages: Seq[RemoteMessageProtocol] = messages

  def filter: Pipeline.Filter = {
    case Some(protocol: RemoteMessageProtocol) if protocol.getActorInfo.getId.equals(id) => {
      messages.append(protocol)
      None
    }
  }
}

case class AddMetaData(id: String, key: String, value: String) {
  val messages = ListBuffer[RemoteMessageProtocol]()

  def interceptedMessages: Seq[RemoteMessageProtocol] = messages

  def filter: Pipeline.Filter = {
    case Some(protocol: RemoteMessageProtocol) if protocol.getActorInfo.getId.equals(id) => {
      messages.append(protocol)
      val bytes = ByteString.copyFromUtf8(value)
      val metadata = MetadataEntryProtocol.newBuilder.setKey(key).setValue(bytes).build
      val newMessage = protocol.toBuilder.addMetadata(metadata).build
      Some(newMessage)
    }
  }
}

case class GetMetaData(id: String) {
  val messages = ListBuffer[RemoteMessageProtocol]()

  def interceptedMessages: Seq[RemoteMessageProtocol] = messages

  def header: (String, String) = (messages.last.getMetadata(0).getKey, messages.last.getMetadata(0).getValue.toStringUtf8())

  def filter: Pipeline.Filter = {
    case Some(protocol: RemoteMessageProtocol) if protocol.getActorInfo.getId.equals(id) => {
      messages.append(protocol)
      Some(protocol)
    }
  }
}


case class Modify(id: String) {
  val messages = ListBuffer[RemoteMessageProtocol]()

  def interceptedMessages: Seq[RemoteMessageProtocol] = messages

  def filter: Pipeline.Filter = {
    case Some(protocol: RemoteMessageProtocol) if protocol.getActorInfo.getId.equals(id) => {
      messages.append(protocol)
      val message = MessageSerializer.serialize(new String("changed the message in the pipeline"))
      Some(protocol.toBuilder.setMessage(message).build)
    }
  }
}

case class PassThrough(id: String) {
  val messages = ListBuffer[RemoteMessageProtocol]()

  def interceptedMessages: Seq[RemoteMessageProtocol] = messages

  def filter: Pipeline.Filter = {
    case Some(protocol: RemoteMessageProtocol) => {
      messages.append(protocol)
      Some(protocol)
    }
  }
}

class EchoActor extends Actor {
  def receive = {
    case msg => {
      self.reply(msg)
    }
  }
}
