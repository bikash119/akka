package remote

import akka.actor.remote.{OptimizedLocalScopedSpec, AkkaRemoteTest}
import akka.actor.Actor
import akka.actor.Actor._
import akka.util.{Logging, TestKit, Address}
import akka.util.Duration._
import akka.util.duration._
import collection.mutable.ListBuffer
import org.scalatest.matchers.ShouldMatchers
import java.lang.String
import akka.remote.netty.NettyRemoteSupport
import org.scalatest.{BeforeAndAfterAll, WordSpec}
import akka.remote.protocol.RemoteProtocol.{SerializationSchemeType, MessageProtocol, RemoteMessageProtocol}
import akka.remote.{MessageSerializer, Pipeline}

class PipelineTest extends WordSpec with ShouldMatchers with BeforeAndAfterAll with TestKit with Logging {
  val remote = Actor.remote
  val host = "localhost"
  val port = 25520
  val timeoutTestActor = 50
  val maxsecs = 2
  val noMsgSecs = 2
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
      within(maxsecs seconds) {
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
  "get the request passed through it and exclude the message with FilterByName" in {
    within(noMsgSecs seconds) {
      val filter = FilterByName(actorName)
      Pipeline.registerClientFilters(Address(host, port), filter.filter)
      filter.interceptedMessages should have size (0)
      remote.actorFor(actorName, host, port) ! "test"
      expectNoMsg
      filter.interceptedMessages should have size (1)
      filter.interceptedMessages map {
        protocol => protocol.getActorInfo.getId should be(actorName)
      }
    }
  }


  "get the request passed through it without FilterByName" in {
    within(maxsecs seconds) {
      val filter = FilterByName(anotherName)
      Pipeline.registerClientFilters(Address(host, port), filter.filter)
      filter.interceptedMessages should have size (0)
      remote.actorFor(actorName, host, port) ! "test"
      expectMsg("test")
      filter.interceptedMessages should have size (0)
    }
  }


  "get the request passed through it and modify the message with Modify" in {
    within(maxsecs seconds) {
      val filter = Modify(actorName)
      Pipeline.registerClientFilters(Address(host, port), filter.filter)
      filter.interceptedMessages should have size (0)
      remote.actorFor(actorName, host, port) ! "test"
      expectMsg("changed the message in the pipeline")
      filter.interceptedMessages should have size (1)
      filter.interceptedMessages map {
        protocol => protocol.getActorInfo.getId should be(actorName)
      }
    }
  }



  "A registered client receive filter" should {
    "get the reply passed through it" in {
      within(maxsecs seconds) {
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
  }


  "A registered server receive filter" should {
    "get the request passed through it" in {
      within(maxsecs seconds) {
        val filter = PassThrough(echoName)
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
    }
  }


  "A registered server send filter" should {
    "get the reply passed through it" in {
      within(maxsecs seconds) {
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

}