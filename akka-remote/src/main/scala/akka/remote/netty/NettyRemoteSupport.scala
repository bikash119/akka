/**
 * Copyright (C) 2009-2011 Scalable Solutions AB <http://scalablesolutions.se>
 */

package akka.remote.netty

import akka.dispatch.{DefaultCompletableFuture, CompletableFuture, Future}
import akka.remote.protocol.RemoteProtocol._
import akka.remote.protocol.RemoteProtocol.ActorType._
import akka.config.ConfigurationException
import akka.serialization.RemoteActorSerialization
import akka.serialization.RemoteActorSerialization._
import akka.japi.Creator
import akka.config.Config._
import akka.remoteinterface._
import akka.actor.{Index, ActorInitializationException, LocalActorRef, newUuid, ActorRegistry, Actor, RemoteActorRef, TypedActor, ActorRef, IllegalActorStateException, RemoteActorSystemMessage, uuidFrom, Uuid, Exit, LifeCycleMessage, ActorType => AkkaActorType}
import akka.AkkaException
import akka.actor.Actor._
import akka.util._
import org.jboss.netty.channel._
import org.jboss.netty.channel.group.{DefaultChannelGroup, ChannelGroup, ChannelGroupFuture}
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory
import org.jboss.netty.bootstrap.{ServerBootstrap, ClientBootstrap}
import org.jboss.netty.handler.codec.frame.{LengthFieldBasedFrameDecoder, LengthFieldPrepender}
import org.jboss.netty.handler.codec.compression.{ZlibDecoder, ZlibEncoder}
import org.jboss.netty.handler.codec.protobuf.{ProtobufDecoder, ProtobufEncoder}
import org.jboss.netty.handler.timeout.ReadTimeoutHandler
import org.jboss.netty.util.{TimerTask, Timeout, HashedWheelTimer}
import org.jboss.netty.handler.ssl.SslHandler

import java.net.{SocketAddress, InetSocketAddress}
import java.util.concurrent.{TimeUnit, Executors, ConcurrentMap, ConcurrentHashMap, ConcurrentSkipListSet}
import scala.collection.mutable.{HashMap}
import scala.reflect.BeanProperty
import java.lang.reflect.InvocationTargetException
import java.util.concurrent.atomic.{AtomicReference, AtomicLong, AtomicBoolean}
import akka.remote._

trait NettyRemoteClientModule extends DefaultRemoteClientModule {
  self: ListenerManagement with Logging =>
  protected[akka] def createClient(address: InetSocketAddress, loader: Option[ClassLoader]): RemoteClient = {
    new ActiveRemoteClient(this, address, loader, self.notifyListeners _)
  }
}

/**
 * This is the abstract baseclass for netty remote clients,
 * currently there's only an ActiveRemoteClient, but otehrs could be feasible, like a PassiveRemoteClient that
 * reuses an already established connection.
 */
abstract class RemoteClient private[akka](
                                             module: RemoteClientModule,
                                             remoteAddress: InetSocketAddress) extends DefaultClient {

  def writeOneWay[T](request: RemoteMessageProtocol): Unit = {
    currentChannel.write(request).addListener(new ChannelFutureListener {
      def operationComplete(future: ChannelFuture) {
        if (future.isCancelled) {
          //We don't care about that right now
        } else if (!future.isSuccess) {
          notifyListeners(RemoteClientWriteFailed(request, future.getCause, module, remoteAddress))
        }
      }
    })
  }

  def writeTwoWay[T](senderFuture: Option[CompletableFuture[T]], request: RemoteMessageProtocol): Some[CompletableFuture[T]] = {
    val futureResult = if (senderFuture.isDefined) senderFuture.get
    else new DefaultCompletableFuture[T](request.getActorInfo.getTimeout)
    val futureUuid = uuidFrom(request.getUuid.getHigh, request.getUuid.getLow)
    futures.put(futureUuid, futureResult) //Add this prematurely, remove it if write fails
    currentChannel.write(request).addListener(new ChannelFutureListener {
      def operationComplete(future: ChannelFuture) {
        if (future.isCancelled) {
          futures.remove(futureUuid) //Clean this up
          //We don't care about that right now
        } else if (!future.isSuccess) {
          futures.remove(futureUuid) //Clean this up
          notifyListeners(RemoteClientWriteFailed(request, future.getCause, module, remoteAddress))
        }
      }
    })
    Some(futureResult)
  }
}

/**
 *  RemoteClient represents a connection to an Akka node. Is used to send messages to remote actors on the node.
 *
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
class ActiveRemoteClient private[akka](
                                          val module: NettyRemoteClientModule, val remoteAddress: InetSocketAddress,
                                          val loader: Option[ClassLoader] = None, notifyListenersFun: (=> Any) => Unit
                                          ) extends RemoteClient(module, remoteAddress) {

  import RemoteClientSettings._

  //FIXME rewrite to a wrapper object (minimize volatile access and maximize encapsulation)
  @volatile private var bootstrap: ClientBootstrap = _
  @volatile private[remote] var connection: ChannelFuture = _
  @volatile private[remote] var openChannels: DefaultChannelGroup = _
  @volatile private var timer: HashedWheelTimer = _
  @volatile private var reconnectionTimeWindowStart = 0L

  def notifyListeners(msg: => Any): Unit = notifyListenersFun(msg)

  def currentChannel = connection.getChannel

  def connect(reconnectIfAlreadyConnected: Boolean = false): Boolean = {
    runSwitch switchOn {
      openChannels = new DefaultDisposableChannelGroup(classOf[RemoteClient].getName)
      timer = new HashedWheelTimer

      bootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(Executors.newCachedThreadPool, Executors.newCachedThreadPool))
      bootstrap.setPipelineFactory(new ActiveRemoteClientPipelineFactory(name, futures, supervisors, bootstrap, remoteAddress, timer, this))
      bootstrap.setOption("tcpNoDelay", true)
      bootstrap.setOption("keepAlive", true)

      log.slf4j.info("Starting remote client connection to [{}]", remoteAddress)

      // Wait until the connection attempt succeeds or fails.
      connection = bootstrap.connect(remoteAddress)
      openChannels.add(connection.awaitUninterruptibly.getChannel)

      if (!connection.isSuccess) {
        notifyListeners(RemoteClientError(connection.getCause, module, remoteAddress))
        log.slf4j.error("Remote client connection to [{}] has failed", remoteAddress)
        log.slf4j.debug("Remote client connection failed", connection.getCause)
        false
      } else {
        timer.newTimeout(new TimerTask() {
          def run(timeout: Timeout) = {
            if (isRunning) {
              log.slf4j.debug("Reaping expired futures awaiting completion from [{}]", remoteAddress)
              val i = futures.entrySet.iterator
              while (i.hasNext) {
                val e = i.next
                if (e.getValue.isExpired)
                  futures.remove(e.getKey)
              }
            }
          }
        }, RemoteClientSettings.REAP_FUTURES_DELAY.length, RemoteClientSettings.REAP_FUTURES_DELAY.unit)
        notifyListeners(RemoteClientStarted(module, remoteAddress))
        true
      }
    } match {
      case true => true
      case false if reconnectIfAlreadyConnected =>
        isAuthenticated.set(false)
        log.slf4j.debug("Remote client reconnecting to [{}]", remoteAddress)
        openChannels.remove(connection.getChannel)
        connection.getChannel.close
        connection = bootstrap.connect(remoteAddress)
        openChannels.add(connection.awaitUninterruptibly.getChannel) // Wait until the connection attempt succeeds or fails.
        if (!connection.isSuccess) {
          notifyListeners(RemoteClientError(connection.getCause, module, remoteAddress))
          log.slf4j.error("Reconnection to [{}] has failed", remoteAddress)
          log.slf4j.debug("Reconnection failed", connection.getCause)
          false
        } else true
      case false => false
    }
  }

  def shutdown = runSwitch switchOff {
    log.slf4j.info("Shutting down {}", name)
    notifyListeners(RemoteClientShutdown(module, remoteAddress))
    timer.stop
    timer = null
    openChannels.close.awaitUninterruptibly
    openChannels = null
    bootstrap.releaseExternalResources
    bootstrap = null
    connection = null
    log.slf4j.info("{} has been shut down", name)
  }

  private[akka] def isWithinReconnectionTimeWindow: Boolean = {
    if (reconnectionTimeWindowStart == 0L) {
      reconnectionTimeWindowStart = System.currentTimeMillis
      true
    } else {
      val timeLeft = RECONNECTION_TIME_WINDOW - (System.currentTimeMillis - reconnectionTimeWindowStart)
      if (timeLeft > 0) {
        log.slf4j.info("Will try to reconnect to remote server for another [{}] milliseconds", timeLeft)
        true
      } else false
    }
  }

  private[akka] def resetReconnectionTimeWindow = reconnectionTimeWindowStart = 0L
}

/**
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
class ActiveRemoteClientPipelineFactory(
                                           name: String,
                                           futures: ConcurrentMap[Uuid, CompletableFuture[_]],
                                           supervisors: ConcurrentMap[Uuid, ActorRef],
                                           bootstrap: ClientBootstrap,
                                           remoteAddress: SocketAddress,
                                           timer: HashedWheelTimer,
                                           client: ActiveRemoteClient) extends ChannelPipelineFactory {

  def getPipeline: ChannelPipeline = {
    def join(ch: ChannelHandler*) = Array[ChannelHandler](ch: _*)

    lazy val engine = {
      val e = RemoteServerSslContext.client.createSSLEngine()
      e.setEnabledCipherSuites(e.getSupportedCipherSuites) //TODO is this sensible?
      e.setUseClientMode(true)
      e
    }

    val ssl = if (RemoteServerSettings.SECURE) join(new SslHandler(engine)) else join()
    val timeout = new ReadTimeoutHandler(timer, RemoteClientSettings.READ_TIMEOUT.toMillis.toInt)
    val lenDec = new LengthFieldBasedFrameDecoder(RemoteClientSettings.MESSAGE_FRAME_SIZE, 0, 4, 0, 4)
    val lenPrep = new LengthFieldPrepender(4)
    val protobufDec = new ProtobufDecoder(RemoteMessageProtocol.getDefaultInstance)
    val protobufEnc = new ProtobufEncoder
    val (enc, dec) = RemoteServerSettings.COMPRESSION_SCHEME match {
      case "zlib" => (join(new ZlibEncoder(RemoteServerSettings.ZLIB_COMPRESSION_LEVEL)), join(new ZlibDecoder))
      case _ => (join(), join())
    }

    val remoteClient = new ActiveRemoteClientHandler(name, futures, supervisors, bootstrap, remoteAddress, timer, client)
    val stages = ssl ++ join(timeout) ++ dec ++ join(lenDec, protobufDec) ++ enc ++ join(lenPrep, protobufEnc, remoteClient)
    new StaticChannelPipeline(stages: _*)
  }
}

/**
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
@ChannelHandler.Sharable
class ActiveRemoteClientHandler(
                                   val name: String,
                                   val futures: ConcurrentMap[Uuid, CompletableFuture[_]],
                                   val supervisors: ConcurrentMap[Uuid, ActorRef],
                                   val bootstrap: ClientBootstrap,
                                   val remoteAddress: SocketAddress,
                                   val timer: HashedWheelTimer,
                                   val client: ActiveRemoteClient)
    extends SimpleChannelUpstreamHandler with Logging {

  override def handleUpstream(ctx: ChannelHandlerContext, event: ChannelEvent) = {
    if (event.isInstanceOf[ChannelStateEvent] &&
        event.asInstanceOf[ChannelStateEvent].getState != ChannelState.INTEREST_OPS) {
      log.slf4j.debug(event.toString)
    }
    super.handleUpstream(ctx, event)
  }

  override def messageReceived(ctx: ChannelHandlerContext, event: MessageEvent) {
    try {
      client.receiveMessage(event.getMessage)
    } catch {
      case e: Exception =>
        client.notifyListeners(RemoteClientError(e, client.module, client.remoteAddress))
        log.slf4j.error("Unexpected exception in remote client handler", e)
        throw e
    }
  }

  override def channelClosed(ctx: ChannelHandlerContext, event: ChannelStateEvent) = client.runSwitch ifOn {
    if (client.isWithinReconnectionTimeWindow) {
      timer.newTimeout(new TimerTask() {
        def run(timeout: Timeout) = {
          if (client.isRunning) {
            client.openChannels.remove(event.getChannel)
            client.connect(reconnectIfAlreadyConnected = true)
          }
        }
      }, RemoteClientSettings.RECONNECT_DELAY.toMillis, TimeUnit.MILLISECONDS)
    } else spawn {
      client.shutdown
    }
  }

  override def channelConnected(ctx: ChannelHandlerContext, event: ChannelStateEvent) = {
    def connect = {
      client.notifyListeners(RemoteClientConnected(client.module, client.remoteAddress))
      log.slf4j.debug("Remote client connected to [{}]", ctx.getChannel.getRemoteAddress)
      client.resetReconnectionTimeWindow
    }

    if (RemoteServerSettings.SECURE) {
      val sslHandler: SslHandler = ctx.getPipeline.get(classOf[SslHandler])
      sslHandler.handshake.addListener(new ChannelFutureListener {
        def operationComplete(future: ChannelFuture): Unit = {
          if (future.isSuccess) connect
          else throw new RemoteClientException("Could not establish SSL handshake", client.module, client.remoteAddress)
        }
      })
    } else connect
  }

  override def channelDisconnected(ctx: ChannelHandlerContext, event: ChannelStateEvent) = {
    client.notifyListeners(RemoteClientDisconnected(client.module, client.remoteAddress))
    log.slf4j.debug("Remote client disconnected from [{}]", ctx.getChannel.getRemoteAddress)
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, event: ExceptionEvent) = {
    client.notifyListeners(RemoteClientError(event.getCause, client.module, client.remoteAddress))
    if (event.getCause ne null)
      log.slf4j.error("Unexpected exception from downstream in remote client", event.getCause)
    else
      log.slf4j.error("Unexpected exception from downstream in remote client: {}", event)

    event.getChannel.close
  }
}

/**
 * Provides the implementation of the Netty remote support
 */
class NettyRemoteSupport extends RemoteSupport with NettyRemoteServerModule with NettyRemoteClientModule {
  //Needed for remote testing and switching on/off under run
  val optimizeLocal = new AtomicBoolean(true)

  def optimizeLocalScoped_?() = optimizeLocal.get

  protected[akka] def actorFor(serviceId: String, className: String, timeout: Long, host: String, port: Int, loader: Option[ClassLoader]): ActorRef = {
    if (optimizeLocalScoped_?) {
      val home = this.address
      if (host == home.getHostName && port == home.getPort) {
        //TODO: switch to InetSocketAddress.equals?
        val localRef = findActorByIdOrUuid(serviceId, serviceId)
        if (localRef ne null) return localRef //Code significantly simpler with the return statement
      }
    }

    RemoteActorRef(serviceId, className, host, port, timeout, loader)
  }

  def clientManagedActorOf(factory: () => Actor, host: String, port: Int): ActorRef = {

    if (optimizeLocalScoped_?) {
      val home = this.address
      if (host == home.getHostName && port == home.getPort) //TODO: switch to InetSocketAddress.equals?
        return new LocalActorRef(factory, None) // Code is much simpler with return
    }

    val ref = new LocalActorRef(factory, Some(new InetSocketAddress(host, port)), clientManaged = true)
    //ref.timeout = timeout //removed because setting default timeout should be done after construction
    ref
  }
}

class NettyRemoteServer(serverModule: RemoteServerModule, val host: String, val port: Int, val loader: Option[ClassLoader]) extends RemoteServer {

  val name = "NettyRemoteServer@" + host + ":" + port
  val address = new InetSocketAddress(host, port)

  private val factory = new NioServerSocketChannelFactory(Executors.newCachedThreadPool, Executors.newCachedThreadPool)

  private val bootstrap = new ServerBootstrap(factory)

  // group of open channels, used for clean-up
  private val openChannels: ChannelGroup = new DefaultDisposableChannelGroup("akka-remote-server")
  val remoteServer = new RemoteServerHandler(name, openChannels, loader, serverModule)
  val pipelineFactory = new RemoteServerPipelineFactory(name, openChannels, loader, serverModule, remoteServer)
  bootstrap.setPipelineFactory(pipelineFactory)
  bootstrap.setOption("backlog", RemoteServerSettings.BACKLOG)
  bootstrap.setOption("child.tcpNoDelay", true)
  bootstrap.setOption("child.keepAlive", true)
  bootstrap.setOption("child.reuseAddress", true)
  bootstrap.setOption("child.connectTimeoutMillis", RemoteServerSettings.CONNECTION_TIMEOUT_MILLIS.toMillis)

  openChannels.add(bootstrap.bind(address))
  serverModule.notifyListeners(RemoteServerStarted(serverModule))

  def shutdown {
    try {
      openChannels.disconnect
      openChannels.close.awaitUninterruptibly
      bootstrap.releaseExternalResources
      serverModule.notifyListeners(RemoteServerShutdown(serverModule))
    } catch {
      case e: java.nio.channels.ClosedChannelException => {}
      case e => serverModule.log.slf4j.warn("Could not close remote server channel in a graceful way")
    }
  }
  protected [akka] def registerServerFilters(sendFilter: Pipeline.Filter, receiveFilter:Pipeline.Filter) = {
    remoteServer.registerFilters(sendFilter, receiveFilter)

  }
  protected [akka] def unregisterServerFilters = {
    remoteServer.unregisterFilters
  }
}

trait NettyRemoteServerModule extends DefaultRemoteServerModule {
  def createRemoteServer(remoteServerModule: RemoteServerModule, hostname: String, port: Int, loader: Option[ClassLoader]): RemoteServer = {
    new NettyRemoteServer(remoteServerModule, hostname, port, loader)
  }
}

object RemoteServerSslContext {

  import javax.net.ssl.SSLContext

  val (client, server) = {
    val protocol = "TLS"
    //val algorithm = Option(Security.getProperty("ssl.KeyManagerFactory.algorithm")).getOrElse("SunX509")
    //val store = KeyStore.getInstance("JKS")
    val s = SSLContext.getInstance(protocol)
    s.init(null, null, null)
    val c = SSLContext.getInstance(protocol)
    c.init(null, null, null)
    (c, s)
  }
}

/**
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
class RemoteServerPipelineFactory(
                                     val name: String,
                                     val openChannels: ChannelGroup,
                                     val loader: Option[ClassLoader],
                                     val server: RemoteServerModule,
                                     val remoteServerHandler: RemoteServerHandler) extends ChannelPipelineFactory {

  import RemoteServerSettings._

  def getPipeline: ChannelPipeline = {
    def join(ch: ChannelHandler*) = Array[ChannelHandler](ch: _*)

    lazy val engine = {
      val e = RemoteServerSslContext.server.createSSLEngine()
      e.setEnabledCipherSuites(e.getSupportedCipherSuites) //TODO is this sensible?
      e.setUseClientMode(false)
      e
    }

    val ssl = if (SECURE) join(new SslHandler(engine)) else join()
    val lenDec = new LengthFieldBasedFrameDecoder(MESSAGE_FRAME_SIZE, 0, 4, 0, 4)
    val lenPrep = new LengthFieldPrepender(4)
    val protobufDec = new ProtobufDecoder(RemoteMessageProtocol.getDefaultInstance)
    val protobufEnc = new ProtobufEncoder
    val (enc, dec) = COMPRESSION_SCHEME match {
      case "zlib" => (join(new ZlibEncoder(ZLIB_COMPRESSION_LEVEL)), join(new ZlibDecoder))
      case _ => (join(), join())
    }
    val stages = ssl ++ dec ++ join(lenDec, protobufDec) ++ enc ++ join(lenPrep, protobufEnc, remoteServerHandler)
    new StaticChannelPipeline(stages: _*)
  }
}

class NettySession(val server: RemoteServerModule, val channel: Channel) extends Session[Channel] {
  def write(message: AnyRef): Unit = {
    channel.write(message).addListener(
      new ChannelFutureListener {
        def operationComplete(future: ChannelFuture): Unit = {
          if (future.isCancelled) {
            //Not interesting at the moment
          } else if (!future.isSuccess) {
            val socketAddress = future.getChannel.getRemoteAddress match {
              case i: InetSocketAddress => Some(i)
              case _ => None
            }
            server.notifyListeners(RemoteServerWriteFailed(message, future.getCause, server, socketAddress))
          }
        }
      })
  }
}

/**
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
@ChannelHandler.Sharable
class RemoteServerHandler(
                             val name: String,
                             val openChannels: ChannelGroup,
                             val applicationLoader: Option[ClassLoader],
                             val server: RemoteServerModule) extends SimpleChannelUpstreamHandler with Logging with RemoteMessageProtocolHandler[Channel] {

  import RemoteServerSettings._

  val CHANNEL_INIT = "channel-init".intern

  val sessionActors = new ChannelLocal[ConcurrentHashMap[String, ActorRef]]()
  val typedSessionActors = new ChannelLocal[ConcurrentHashMap[String, AnyRef]]()

  def findSessionActor(id: String, channel: Channel): ActorRef = {
    sessionActors.get(channel) match {
      case null => null
      case map => map get id
    }
  }

  def findTypedSessionActor(id: String, channel: Channel): AnyRef = {
    typedSessionActors.get(channel) match {
      case null => null
      case map => map get id
    }
  }

  def getTypedSessionActors(channel: Channel): ConcurrentHashMap[String, AnyRef] = {
    typedSessionActors.get(channel)
  }

  def getSessionActors(channel: Channel): ConcurrentHashMap[String, ActorRef] = {
    sessionActors.get(channel)
  }

  /**
   * ChannelOpen overridden to store open channels for a clean postStop of a node.
   * If a channel is closed before, it is automatically removed from the open channels group.
   */
  override def channelOpen(ctx: ChannelHandlerContext, event: ChannelStateEvent) = openChannels.add(ctx.getChannel)

  override def channelConnected(ctx: ChannelHandlerContext, event: ChannelStateEvent) = {
    val clientAddress = getClientAddress(ctx)
    sessionActors.set(event.getChannel(), new ConcurrentHashMap[String, ActorRef]())
    typedSessionActors.set(event.getChannel(), new ConcurrentHashMap[String, AnyRef]())
    log.slf4j.debug("Remote client [{}] connected to [{}]", clientAddress, server.name)
    if (SECURE) {
      val sslHandler: SslHandler = ctx.getPipeline.get(classOf[SslHandler])
      // Begin handshake.
      sslHandler.handshake().addListener(new ChannelFutureListener {
        def operationComplete(future: ChannelFuture): Unit = {
          if (future.isSuccess) {
            openChannels.add(future.getChannel)
            server.notifyListeners(RemoteServerClientConnected(server, clientAddress))
          } else future.getChannel.close
        }
      })
    } else {
      server.notifyListeners(RemoteServerClientConnected(server, clientAddress))
    }
    if (REQUIRE_COOKIE) ctx.setAttachment(CHANNEL_INIT) // signal that this is channel initialization, which will need authentication
  }

  override def channelDisconnected(ctx: ChannelHandlerContext, event: ChannelStateEvent) = {
    import scala.collection.JavaConversions.asScalaIterable
    val clientAddress = getClientAddress(ctx)
    log.slf4j.debug("Remote client [{}] disconnected from [{}]", clientAddress, server.name)

    // stop all session actors
    for (map <- Option(sessionActors.remove(event.getChannel));
         actor <- asScalaIterable(map.values)) {
      try {
        actor.stop
      } catch {
        case e: Exception => log.slf4j.warn("Couldn't stop {}", actor, e)
      }
    }
    // stop all typed session actors
    for (map <- Option(typedSessionActors.remove(event.getChannel));
         actor <- asScalaIterable(map.values)) {
      try {
        TypedActor.stop(actor)
      } catch {
        case e: Exception => log.slf4j.warn("Couldn't stop {}", actor, e)
      }
    }

    server.notifyListeners(RemoteServerClientDisconnected(server, clientAddress))
  }

  override def channelClosed(ctx: ChannelHandlerContext, event: ChannelStateEvent) = {
    val clientAddress = getClientAddress(ctx)
    log.slf4j.debug("Remote client [{}] channel closed from [{}]", clientAddress, server.name)
    server.notifyListeners(RemoteServerClientClosed(server, clientAddress))
  }

  override def handleUpstream(ctx: ChannelHandlerContext, event: ChannelEvent) = {
    if (event.isInstanceOf[ChannelStateEvent] && event.asInstanceOf[ChannelStateEvent].getState != ChannelState.INTEREST_OPS) {
      log.slf4j.debug(event.toString)
    }
    super.handleUpstream(ctx, event)
  }

  override def messageReceived(ctx: ChannelHandlerContext, event: MessageEvent) = event.getMessage match {
    case null => throw new IllegalActorStateException("Message in remote MessageEvent is null: " + event)
    case requestProtocol: RemoteMessageProtocol =>
      if (REQUIRE_COOKIE) authenticateRemoteClient(requestProtocol, ctx)
      handleRemoteMessageProtocol(requestProtocol, new NettySession(server, event.getChannel))
    case _ => //ignore
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, event: ExceptionEvent) = {
    log.slf4j.error("Unexpected exception from remote downstream", event.getCause)
    event.getChannel.close
    server.notifyListeners(RemoteServerError(event.getCause, server))
  }

  private def getClientAddress(ctx: ChannelHandlerContext): Option[InetSocketAddress] =
    ctx.getChannel.getRemoteAddress match {
      case inet: InetSocketAddress => Some(inet)
      case _ => None
    }

  private def authenticateRemoteClient(request: RemoteMessageProtocol, ctx: ChannelHandlerContext) = {
    val attachment = ctx.getAttachment
    if ((attachment ne null) &&
        attachment.isInstanceOf[String] &&
        attachment.asInstanceOf[String] == CHANNEL_INIT) {
      // is first time around, channel initialization
      ctx.setAttachment(null)
      val clientAddress = ctx.getChannel.getRemoteAddress.toString
      if (!request.hasCookie) throw new SecurityException(
        "The remote client [" + clientAddress + "] does not have a secure cookie.")
      if (!(request.getCookie == SECURE_COOKIE.get)) throw new SecurityException(
        "The remote client [" + clientAddress + "] secure cookie is not the same as remote server secure cookie")
      log.slf4j.info("Remote client [{}] successfully authenticated using secure cookie", clientAddress)
    }
  }
}

class DefaultDisposableChannelGroup(name: String) extends DefaultChannelGroup(name) {
  protected val guard = new ReadWriteGuard
  protected val open = new AtomicBoolean(true)

  override def add(channel: Channel): Boolean = guard withReadGuard {
    if (open.get) {
      super.add(channel)
    } else {
      channel.close
      false
    }
  }

  override def close(): ChannelGroupFuture = guard withWriteGuard {
    if (open.getAndSet(false)) {
      super.close
    } else {
      throw new IllegalStateException("ChannelGroup already closed, cannot add new channel")
    }
  }
}
