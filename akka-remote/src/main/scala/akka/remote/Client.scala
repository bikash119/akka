package akka.remote

import akka.dispatch.CompletableFuture
import java.net.InetSocketAddress
import protocol.RemoteProtocol.RemoteMessageProtocol
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import org.jboss.netty.channel.Channel
import akka.remoteinterface._
import scala.collection.mutable.HashMap
import akka.util._
import akka.actor.{TypedActor, Index, newUuid, Uuid, IllegalActorStateException, ActorRef, ActorType => AkkaActorType, RemoteActorRef}
import akka.serialization.RemoteActorSerialization

trait Client {
  def connect(reconnectIfAlreadyConnected: Boolean = false): Boolean

  def shutdown: Boolean

  /**
   * Converts the message to the wireprotocol and sends the message across the wire
   */
  def send[T](
                 message: Any,
                 senderOption: Option[ActorRef],
                 senderFuture: Option[CompletableFuture[T]],
                 remoteAddress: InetSocketAddress,
                 timeout: Long,
                 isOneWay: Boolean,
                 actorRef: ActorRef,
                 typedActorInfo: Option[Tuple2[String, String]],
                 actorType: AkkaActorType): Option[CompletableFuture[T]]

  def send[T](
                 request: RemoteMessageProtocol,
                 senderFuture: Option[CompletableFuture[T]]): Option[CompletableFuture[T]]

  protected def notifyListeners(msg: => Any);

  protected[akka] def registerSupervisorForActor(actorRef: ActorRef): ActorRef

  protected[akka] def deregisterSupervisorForActor(actorRef: ActorRef): ActorRef
}

trait AbstractClient extends Logging with Client {
  protected val remoteAddress: InetSocketAddress
  protected val module: RemoteClientModule
  private val fallback: PartialFunction[Option[RemoteMessageProtocol], Option[RemoteMessageProtocol]] = {case Some(msg:RemoteMessageProtocol) => Some(msg) case _ => None}
  private var filter: PartialFunction[Option[RemoteMessageProtocol], Option[RemoteMessageProtocol]] = fallback

  val name = this.getClass.getSimpleName + "@" + remoteAddress.getHostName + "::" + remoteAddress.getPort

  protected val futures = new ConcurrentHashMap[Uuid, CompletableFuture[_]]
  protected val supervisors = new ConcurrentHashMap[Uuid, ActorRef]
  private[remote] val runSwitch = new Switch()
  private[remote] val isAuthenticated = new AtomicBoolean(false)

  private[remote] def isRunning = runSwitch.isOn

  protected def notifyListeners(msg: => Any);
  Unit

  protected def currentChannel: Channel

  def connect(reconnectIfAlreadyConnected: Boolean = false): Boolean

  def shutdown: Boolean

  /**
   * Converts the message to the wireprotocol and sends the message across the wire
   */
  def send[T](
                 message: Any,
                 senderOption: Option[ActorRef],
                 senderFuture: Option[CompletableFuture[T]],
                 remoteAddress: InetSocketAddress,
                 timeout: Long,
                 isOneWay: Boolean,
                 actorRef: ActorRef,
                 typedActorInfo: Option[Tuple2[String, String]],
                 actorType: AkkaActorType): Option[CompletableFuture[T]] = synchronized {
    //TODO: find better strategy to prevent race
    val remoteProtocolMessage  = RemoteActorSerialization.createRemoteMessageProtocolBuilder(
      Some(actorRef),
      Left(actorRef.uuid),
      actorRef.id,
      actorRef.actorClassName,
      actorRef.timeout,
      Left(message),
      isOneWay,
      senderOption,
      typedActorInfo,
      actorType,
      if (isAuthenticated.compareAndSet(false, true)) RemoteClientSettings.SECURE_COOKIE else None
    ).build

    (filter orElse fallback) (Some(remoteProtocolMessage)) flatMap (send(_, senderFuture))
  }

  /**
   * Sends the message across the wire
   */
  def writeOneWay[T](request: RemoteMessageProtocol): Unit

  def writeTwoWay[T](senderFuture: Option[CompletableFuture[T]], request: RemoteMessageProtocol): Some[CompletableFuture[T]]

  def send[T](   request: RemoteMessageProtocol,
                 senderFuture: Option[CompletableFuture[T]]): Option[CompletableFuture[T]] = {
    log.slf4j.debug("sending message: {} has future {}", request, senderFuture)
    if (isRunning) {
      if (request.getOneWay) {
        writeOneWay(request)
        None
      } else {
        writeTwoWay(senderFuture, request)
      }
    } else {
      val exception = new RemoteClientException("Remote client is not running, make sure you have invoked 'RemoteClient.connect' before using it.", module, remoteAddress)
      notifyListeners(RemoteClientError(exception, module, remoteAddress))
      throw exception
    }
  }

  protected[akka] def registerSupervisorForActor(actorRef: ActorRef): ActorRef =
    if (!actorRef.supervisor.isDefined) throw new IllegalActorStateException(
      "Can't register supervisor for " + actorRef + " since it is not under supervision")
    else supervisors.putIfAbsent(actorRef.supervisor.get.uuid, actorRef)

  protected[akka] def deregisterSupervisorForActor(actorRef: ActorRef): ActorRef =
    if (!actorRef.supervisor.isDefined) throw new IllegalActorStateException(
      "Can't unregister supervisor for " + actorRef + " since it is not under supervision")
    else supervisors.remove(actorRef.supervisor.get.uuid)
}

trait DefaultRemoteClientModule extends RemoteClientModule {
  self: ListenerManagement with Logging =>
  private val remoteClients = new HashMap[Address, Client]
  private val remoteActors = new Index[Address, Uuid]
  private val lock = new ReadWriteGuard

  protected[akka] def typedActorFor[T](intfClass: Class[T], serviceId: String, implClassName: String, timeout: Long, hostname: String, port: Int, loader: Option[ClassLoader]): T =
    TypedActor.createProxyForRemoteActorRef(intfClass, RemoteActorRef(serviceId, implClassName, hostname, port, timeout, loader, AkkaActorType.TypedActor))

  protected[akka] def send[T](message: Any,
                              senderOption: Option[ActorRef],
                              senderFuture: Option[CompletableFuture[T]],
                              remoteAddress: InetSocketAddress,
                              timeout: Long,
                              isOneWay: Boolean,
                              actorRef: ActorRef,
                              typedActorInfo: Option[Tuple2[String, String]],
                              actorType: AkkaActorType,
                              loader: Option[ClassLoader]): Option[CompletableFuture[T]] =
    withClientFor(remoteAddress, loader) (_.send[T](message, senderOption, senderFuture, remoteAddress, timeout, isOneWay, actorRef, typedActorInfo, actorType))

  protected[akka] def createClient(address: InetSocketAddress, loader: Option[ClassLoader]): Client

  protected[akka] def withClientFor[T](
                                          address: InetSocketAddress, loader: Option[ClassLoader])(fun: Client => T): T = {
    loader.foreach(MessageSerializer.setClassLoader(_)) //TODO: REVISIT: THIS SMELLS FUNNY
    val key = Address(address)
    lock.readLock.lock
    try {
      val c = remoteClients.get(key) match {
        case Some(client) => client
        case None =>
          lock.readLock.unlock
          lock.writeLock.lock //Lock upgrade, not supported natively
          try {
            try {
              remoteClients.get(key) match {
              //Recheck for addition, race between upgrades
                case Some(client) => client //If already populated by other writer
                case None => //Populate map
                  val client = createClient(address, loader)
                  client.connect()
                  remoteClients += key -> client
                  client
              }
            } finally {
              lock.readLock.lock
            } //downgrade
          } finally {
            lock.writeLock.unlock
          }
      }
      fun(c)
    } finally {
      lock.readLock.unlock
    }
  }

  def shutdownClientConnection(address: InetSocketAddress): Boolean = lock withWriteGuard {
    remoteClients.remove(Address(address)) match {
      case Some(client) => client.shutdown
      case None => false
    }
  }

  def restartClientConnection(address: InetSocketAddress): Boolean = lock withReadGuard {
    remoteClients.get(Address(address)) match {
      case Some(client) => client.connect(reconnectIfAlreadyConnected = true)
      case None => false
    }
  }

  private[akka] def registerSupervisorForActor(actorRef: ActorRef): ActorRef =
    withClientFor(actorRef.homeAddress.get, None)(_.registerSupervisorForActor(actorRef))

  private[akka] def deregisterSupervisorForActor(actorRef: ActorRef): ActorRef = lock withReadGuard {
    remoteClients.get(Address(actorRef.homeAddress.get)) match {
      case Some(client) => client.deregisterSupervisorForActor(actorRef)
      case None => actorRef
    }
  }

  /**
   * Clean-up all open connections.
   */
  def shutdownClientModule = {
    shutdownRemoteClients
    //TODO: Should we empty our remoteActors too?
    //remoteActors.clear
  }

  def shutdownRemoteClients = lock withWriteGuard {
    remoteClients.foreach({
      case (addr, client) => client.shutdown
    })
    remoteClients.clear
  }

  def registerClientManagedActor(hostname: String, port: Int, uuid: Uuid) = {
    remoteActors.put(Address(hostname, port), uuid)
  }

  private[akka] def unregisterClientManagedActor(hostname: String, port: Int, uuid: Uuid) = {
    remoteActors.remove(Address(hostname, port), uuid)
    //TODO: should the connection be closed when the last actor deregisters?
  }
}
