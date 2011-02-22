/**
 * Copyright (C) 2009-2011 Scalable Solutions AB <http://scalablesolutions.se>
 */

package akka.remote

import akka.dispatch.CompletableFuture
import java.net.InetSocketAddress
import akka.remote.protocol.RemoteProtocol.RemoteMessageProtocol
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import org.jboss.netty.channel.Channel
import akka.remoteinterface._
import scala.collection.mutable.HashMap
import akka.util._
import akka.actor.{TypedActor, Index, newUuid, Uuid, uuidFrom, Exit, IllegalActorStateException, ActorRef, ActorType => AkkaActorType, RemoteActorRef}
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

  protected[akka] def registerFilters(sendFilter: Pipeline.Filter, receiveFilter: Pipeline.Filter): Unit

  protected[akka] def unregisterFilters(): Unit

  protected def notifyListeners(msg: => Any);

  protected[akka] def registerSupervisorForActor(actorRef: ActorRef): ActorRef

  protected[akka] def deregisterSupervisorForActor(actorRef: ActorRef): ActorRef
}

trait DefaultClient extends Logging with Client {
  protected val remoteAddress: InetSocketAddress
  protected val module: RemoteClientModule
  protected val loader: Option[ClassLoader]
  protected val fallback: Pipeline.Filter = {
    case m => m
  }
  protected var sendFilter: Pipeline.Filter = fallback
  protected var receiveFilter: Pipeline.Filter = fallback

  val name = this.getClass.getSimpleName + "@" + remoteAddress.getHostName + "::" + remoteAddress.getPort

  protected val futures = new ConcurrentHashMap[Uuid, CompletableFuture[_]]
  protected val supervisors = new ConcurrentHashMap[Uuid, ActorRef]
  private[remote] val runSwitch = new Switch()
  private[remote] val isAuthenticated = new AtomicBoolean(false)

  private[remote] def isRunning = runSwitch.isOn

  protected def notifyListeners(msg: => Any);
  Unit

  protected def currentChannel: Channel

  def registerFilters(sendFilter: Pipeline.Filter, receiveFilter: Pipeline.Filter): Unit = {
    this.sendFilter = (sendFilter orElse fallback)
    this.receiveFilter = (receiveFilter orElse fallback)
  }

  def unregisterFilters: Unit = {
    this.sendFilter = fallback
    this.receiveFilter = fallback
  }

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
    val remoteProtocolMessage = RemoteActorSerialization.createRemoteMessageProtocolBuilder(
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

    (sendFilter orElse fallback)(Some(remoteProtocolMessage)) flatMap (send(_, senderFuture))
  }

  def receiveMessage(message: AnyRef): Unit = {
    message match {

      case reply: RemoteMessageProtocol =>
        val replyUuid = uuidFrom(reply.getActorInfo.getUuid.getHigh, reply.getActorInfo.getUuid.getLow)
        log.slf4j.debug("Remote client received RemoteMessageProtocol[\n{}]", reply)
        log.slf4j.debug("Trying to map back to future: {}", replyUuid)
        val future = futures.remove(replyUuid).asInstanceOf[CompletableFuture[Any]]
        (receiveFilter orElse fallback)(Some(reply)) map {
          filteredReply =>

            if (filteredReply.hasMessage) {
              if (future eq null) throw new IllegalActorStateException("Future mapped to UUID " + replyUuid + " does not exist")
              val message = MessageSerializer.deserialize(filteredReply.getMessage)
              future.completeWithResult(message)
            } else {
              val exception = parseException(filteredReply, loader)

              if (filteredReply.hasSupervisorUuid()) {
                val supervisorUuid = uuidFrom(filteredReply.getSupervisorUuid.getHigh, filteredReply.getSupervisorUuid.getLow)
                if (!supervisors.containsKey(supervisorUuid)) throw new IllegalActorStateException(
                  "Expected a registered supervisor for UUID [" + supervisorUuid + "] but none was found")
                val supervisedActor = supervisors.get(supervisorUuid)
                if (!supervisedActor.supervisor.isDefined) throw new IllegalActorStateException(
                  "Can't handle restart for remote actor " + supervisedActor + " since its supervisor has been removed")
                else supervisedActor.supervisor.get ! Exit(supervisedActor, exception)
              }

              future.completeWithException(exception)
            }
        }
      case other =>
        throw new RemoteClientException("Unknown message received in remote client handler: " + other, module, remoteAddress)
    }

  }

  private def parseException(reply: RemoteMessageProtocol, loader: Option[ClassLoader]): Throwable = {
    val exception = reply.getException
    val classname = exception.getClassname
    try {
      val exceptionClass = if (loader.isDefined) loader.get.loadClass(classname)
      else Class.forName(classname)
      exceptionClass
          .getConstructor(Array[Class[_]](classOf[String]): _*)
          .newInstance(exception.getMessage).asInstanceOf[Throwable]
    } catch {
      case problem =>
        log.debug("Couldn't parse exception returned from RemoteServer", problem)
        log.warn("Couldn't create instance of {} with message {}, returning UnparsableException", classname, exception.getMessage)
        UnparsableException(classname, exception.getMessage)
    }
  }

  /**
   * Sends the message across the wire
   */
  def writeOneWay[T](request: RemoteMessageProtocol): Unit

  def writeTwoWay[T](senderFuture: Option[CompletableFuture[T]], request: RemoteMessageProtocol): Some[CompletableFuture[T]]

  def send[T](request: RemoteMessageProtocol,
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

trait ClientFilters {
  private[akka] def registerClientFilters(sendFilter: Pipeline.Filter, receiveFilter: Pipeline.Filter, address: Address, loader: Option[ClassLoader] = None)

  private[akka] def unregisterClientFilters(address: Address, loader: Option[ClassLoader] = None)
}

trait DefaultRemoteClientModule extends RemoteClientModule with ClientFilters {
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
    withClientFor(remoteAddress, loader)(_.send[T](message, senderOption, senderFuture, remoteAddress, timeout, isOneWay, actorRef, typedActorInfo, actorType))

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
                case None => {
                  //Populate map
                  val client = createClient(address, loader)
                  client.connect()
                  remoteClients += key -> client
                  client
                }
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

  private[akka] def registerClientFilters(sendFilter: Pipeline.Filter, receiveFilter: Pipeline.Filter, remoteAddress: Address, loader: Option[ClassLoader] = None): Unit = {
    withClientFor(new InetSocketAddress(remoteAddress.hostname, remoteAddress.port), loader)(_.registerFilters(sendFilter, receiveFilter))
  }

  private[akka] def unregisterClientFilters(remoteAddress: Address, loader: Option[ClassLoader] = None): Unit = {
    withClientFor(new InetSocketAddress(remoteAddress.hostname, remoteAddress.port), loader)(_.unregisterFilters())
  }
}
