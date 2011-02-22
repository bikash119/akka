package akka.remote

import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.ConcurrentHashMap
import java.net.InetSocketAddress
import akka.util.{Logging, Address, ReflectiveAccess, Switch}
import akka.serialization.RemoteActorSerialization
import java.lang.reflect.InvocationTargetException
import akka.dispatch.{Future, DefaultCompletableFuture}
import akka.actor.{TypedActor, Actor, LifeCycleMessage, RemoteActorSystemMessage, IllegalActorStateException, ActorRef, ActorType => AkkaActorType, uuidFrom, Uuid}
import akka.remote.protocol.RemoteProtocol._
import akka.remote.protocol.RemoteProtocol.ActorType._
import akka.remoteinterface.{RemoteSupport, RemoteModule, RemoteServerModule, RemoteServerError}

trait RemoteServer extends ServerFilters {
  def name: String

  def host: String

  def port: Int

  def address: InetSocketAddress

  def shutdown
}

trait ServerFilters {
  protected[akka] def registerServerFilters(sendFilter: Pipeline.Filter, receiveFilter: Pipeline.Filter)

  protected[akka] def unregisterServerFilters
}

trait DefaultRemoteServerModule extends RemoteServerModule with ServerFilters {
  self: RemoteModule =>

  import RemoteServerSettings._

  private[akka] val currentServer = new AtomicReference[Option[RemoteServer]](None)

  def address = currentServer.get match {
    case Some(s) => s.address
    case None => ReflectiveAccess.Remote.configDefaultAddress
  }

  def name = currentServer.get match {
    case Some(s) => s.name
    case None =>
      val transport = ReflectiveAccess.Remote.TRANSPORT
      val transportName = transport.split('.').last
      val a = ReflectiveAccess.Remote.configDefaultAddress
      transportName + "@" + a.getHostName + ":" + a.getPort
  }

  private val _isRunning = new Switch(false)

  def isRunning = _isRunning.isOn

  def createRemoteServer(remoteServerModule: RemoteServerModule, hostname: String, port: Int, loader: Option[ClassLoader]): RemoteServer

  def start(_hostname: String, _port: Int, loader: Option[ClassLoader] = None): RemoteServerModule = guard withGuard {
    try {
      _isRunning switchOn {
        log.slf4j.debug("Starting up remote server on {}:{}", _hostname, _port)
        currentServer.set(Some(createRemoteServer(this, _hostname, _port, loader)))
      }
    } catch {
      case e =>
        log.slf4j.error("Could not start up remote server", e)
        notifyListeners(RemoteServerError(e, this))
    }
    this
  }

  def shutdownServerModule = guard withGuard {
    _isRunning switchOff {
      currentServer.getAndSet(None) foreach {
        instance =>
          log.slf4j.debug("Shutting down remote server on {}:{}", instance.host, instance.port)
          instance.shutdown
      }
    }
  }

  /**
   * Register remote typed actor by a specific id.
   * @param id custom actor id
   * @param typedActor typed actor to register
   */
  def registerTypedActor(id: String, typedActor: AnyRef): Unit = guard withGuard {
    log.slf4j.debug("Registering server side remote typed actor [{}] with id [{}]", typedActor.getClass.getName, id)
    if (id.startsWith(UUID_PREFIX)) registerTypedActor(id.substring(UUID_PREFIX.length), typedActor, typedActorsByUuid)
    else registerTypedActor(id, typedActor, typedActors)
  }

  /**
   * Register remote typed actor by a specific id.
   * @param id custom actor id
   * @param typedActor typed actor to register
   */
  def registerTypedPerSessionActor(id: String, factory: => AnyRef): Unit = guard withGuard {
    log.slf4j.debug("Registering server side typed remote session actor with id [{}]", id)
    registerTypedPerSessionActor(id, () => factory, typedActorsFactories)
  }

  /**
   * Register Remote Actor by a specific 'id' passed as argument.
   * <p/>
   * NOTE: If you use this method to register your remote actor then you must unregister the actor by this ID yourself.
   */
  def register(id: String, actorRef: ActorRef): Unit = guard withGuard {
    log.slf4j.debug("Registering server side remote actor [{}] with id [{}]", actorRef.actorClass.getName, id)
    if (id.startsWith(UUID_PREFIX)) register(id.substring(UUID_PREFIX.length), actorRef, actorsByUuid)
    else register(id, actorRef, actors)
  }

  def registerByUuid(actorRef: ActorRef): Unit = guard withGuard {
    log.slf4j.debug("Registering remote actor {} to it's uuid {}", actorRef, actorRef.uuid)
    register(actorRef.uuid.toString, actorRef, actorsByUuid)
  }

  private def register[Key](id: Key, actorRef: ActorRef, registry: ConcurrentHashMap[Key, ActorRef]) {
    if (_isRunning.isOn) {
      registry.put(id, actorRef) //TODO change to putIfAbsent
      if (!actorRef.isRunning) actorRef.start
    }
  }

  /**
   * Register Remote Session Actor by a specific 'id' passed as argument.
   * <p/>
   * NOTE: If you use this method to register your remote actor then you must unregister the actor by this ID yourself.
   */
  def registerPerSession(id: String, factory: => ActorRef): Unit = synchronized {
    log.slf4j.debug("Registering server side remote session actor with id [{}]", id)
    registerPerSession(id, () => factory, actorsFactories)
  }

  private def registerPerSession[Key](id: Key, factory: () => ActorRef, registry: ConcurrentHashMap[Key, () => ActorRef]) {
    if (_isRunning.isOn)
      registry.put(id, factory) //TODO change to putIfAbsent
  }

  private def registerTypedActor[Key](id: Key, typedActor: AnyRef, registry: ConcurrentHashMap[Key, AnyRef]) {
    if (_isRunning.isOn)
      registry.put(id, typedActor) //TODO change to putIfAbsent
  }

  private def registerTypedPerSessionActor[Key](id: Key, factory: () => AnyRef, registry: ConcurrentHashMap[Key, () => AnyRef]) {
    if (_isRunning.isOn)
      registry.put(id, factory) //TODO change to putIfAbsent
  }

  /**
   * Unregister Remote Actor that is registered using its 'id' field (not custom ID).
   */
  def unregister(actorRef: ActorRef): Unit = guard withGuard {
    if (_isRunning.isOn) {
      log.slf4j.debug("Unregistering server side remote actor [{}] with id [{}:{}]", Array[AnyRef](actorRef.actorClass.getName, actorRef.id, actorRef.uuid))
      actors.remove(actorRef.id, actorRef)
      actorsByUuid.remove(actorRef.uuid, actorRef)
    }
  }

  /**
   * Unregister Remote Actor by specific 'id'.
   * <p/>
   * NOTE: You need to call this method if you have registered an actor by a custom ID.
   */
  def unregister(id: String): Unit = guard withGuard {
    if (_isRunning.isOn) {
      log.slf4j.info("Unregistering server side remote actor with id [{}]", id)
      if (id.startsWith(UUID_PREFIX)) actorsByUuid.remove(id.substring(UUID_PREFIX.length))
      else {
        val actorRef = actors get id
        actorsByUuid.remove(actorRef.uuid, actorRef)
        actors.remove(id, actorRef)
      }
    }
  }

  /**
   * Unregister Remote Actor by specific 'id'.
   * <p/>
   * NOTE: You need to call this method if you have registered an actor by a custom ID.
   */
  def unregisterPerSession(id: String): Unit = {
    if (_isRunning.isOn) {
      log.slf4j.info("Unregistering server side remote session actor with id [{}]", id)
      actorsFactories.remove(id)
    }
  }

  /**
   * Unregister Remote Typed Actor by specific 'id'.
   * <p/>
   * NOTE: You need to call this method if you have registered an actor by a custom ID.
   */
  def unregisterTypedActor(id: String): Unit = guard withGuard {
    if (_isRunning.isOn) {
      log.slf4j.info("Unregistering server side remote typed actor with id [{}]", id)
      if (id.startsWith(UUID_PREFIX)) typedActorsByUuid.remove(id.substring(UUID_PREFIX.length))
      else typedActors.remove(id)
    }
  }

  /**
   * Unregister Remote Typed Actor by specific 'id'.
   * <p/>
   * NOTE: You need to call this method if you have registered an actor by a custom ID.
   */
  def unregisterTypedPerSessionActor(id: String): Unit =
    if (_isRunning.isOn) typedActorsFactories.remove(id)

  protected[akka] def registerServerFilters(sendFilter: Pipeline.Filter, receiveFilter: Pipeline.Filter) = {
    if (_isRunning.isOn) {
      currentServer.get.map {
        server =>
          server.registerServerFilters(sendFilter, receiveFilter);
      }
    }
  }

  protected[akka] def unregisterServerFilters = {
    if (_isRunning.isOn) {
      currentServer.get.map {
        server =>
          server.unregisterServerFilters
      }
    }
  }
}

trait Session[T <: Comparable[T]] {
  def write(message: AnyRef): Unit

  def channel: T
}

// handles remote Message protocol and dispatches to Actors, T is the Channel type key for sessions
trait RemoteMessageProtocolHandler[T <: Comparable[T]] extends Logging {

  import RemoteServerSettings._

  val fallback: Pipeline.Filter = {
    case msg => msg
  }
  protected var sendFilter: PartialFunction[Option[RemoteMessageProtocol], Option[RemoteMessageProtocol]] = fallback
  protected var receiveFilter: PartialFunction[Option[RemoteMessageProtocol], Option[RemoteMessageProtocol]] = fallback

  val applicationLoader: Option[ClassLoader]
  val server: RemoteServerModule

  def findSessionActor(id: String, channel: T): ActorRef

  def findTypedSessionActor(id: String, channel: T): AnyRef

  def getTypedSessionActors(channel: T): ConcurrentHashMap[String, AnyRef]

  def getSessionActors(channel: T): ConcurrentHashMap[String, ActorRef]

  applicationLoader.foreach(MessageSerializer.setClassLoader(_))

  //TODO: REVISIT: THIS FEELS A BIT DODGY


  private[akka] def handleRemoteMessageProtocol(request: RemoteMessageProtocol, session: Session[T]) = {
    log.slf4j.debug("Received RemoteMessageProtocol[\n{}]", request)
    (receiveFilter orElse fallback)(Some(request)) map {
      filteredRequest =>
        filteredRequest.getActorInfo.getActorType match {
          case SCALA_ACTOR => dispatchToActor(filteredRequest, session)
          case TYPED_ACTOR => dispatchToTypedActor(filteredRequest, session)
          case JAVA_ACTOR => throw new IllegalActorStateException("ActorType JAVA_ACTOR is currently not supported")
          case other => throw new IllegalActorStateException("Unknown ActorType [" + other + "]")
        }
    }
  }

  private[akka] def dispatchToActor(request: RemoteMessageProtocol, session: Session[T]) {
    val actorInfo = request.getActorInfo
    log.slf4j.debug("Dispatching to remote actor [{}:{}]", actorInfo.getTarget, actorInfo.getUuid)

    val actorRef =
      try {
        createActor(actorInfo, session.channel).start
      } catch {
        case e: SecurityException =>
          session.write(createErrorReplyMessage(e, request, AkkaActorType.ScalaActor))
          server.notifyListeners(RemoteServerError(e, server))
          return
      }

    val message = MessageSerializer.deserialize(request.getMessage)
    val sender =
      if (request.hasSender) Some(RemoteActorSerialization.fromProtobufToRemoteActorRef(request.getSender, applicationLoader))
      else None

    message match {
    // first match on system messages
      case RemoteActorSystemMessage.Stop =>
        if (UNTRUSTED_MODE) throw new SecurityException("Remote server is operating is untrusted mode, can not stop the actor")
        else actorRef.stop
      case _: LifeCycleMessage if (UNTRUSTED_MODE) =>
        throw new SecurityException("Remote server is operating is untrusted mode, can not pass on a LifeCycleMessage to the remote actor")

      case _ => // then match on user defined messages
        if (request.getOneWay) actorRef.!(message)(sender)
        else actorRef.postMessageToMailboxAndCreateFutureResultWithTimeout(
          message,
          request.getActorInfo.getTimeout,
          None,
          Some(new DefaultCompletableFuture[AnyRef](request.getActorInfo.getTimeout).
              onComplete(f => {
            log.slf4j.debug("Future was completed, now flushing to remote!")
            val result = f.result
            val exception = f.exception

            if (exception.isDefined) {
              log.slf4j.debug("Returning exception from actor invocation [{}]", exception.get.getClass)
              session.write(createErrorReplyMessage(exception.get, request, AkkaActorType.ScalaActor))
            }
            else if (result.isDefined) {

              log.slf4j.debug("Returning result from actor invocation [{}]", result.get)
              val messageBuilder = RemoteActorSerialization.createRemoteMessageProtocolBuilder(
                Some(actorRef),
                Right(request.getUuid),
                actorInfo.getId,
                actorInfo.getTarget,
                actorInfo.getTimeout,
                Left(result.get),
                true,
                Some(actorRef),
                None,
                AkkaActorType.ScalaActor,
                None)

              // FIXME lift in the supervisor uuid management into toh createRemoteMessageProtocolBuilder method
              if (request.hasSupervisorUuid) messageBuilder.setSupervisorUuid(request.getSupervisorUuid)
              val protocol = messageBuilder.build
              (sendFilter orElse fallback)(Some(protocol)) map { session.write(_) }
            }
          }
          )
          ))
    }
  }

  private[akka] def dispatchToTypedActor(request: RemoteMessageProtocol, session: Session[T]) = {
    val actorInfo = request.getActorInfo
    val typedActorInfo = actorInfo.getTypedActorInfo
    log.slf4j.debug("Dispatching to remote typed actor [{} :: {}]", typedActorInfo.getMethod, typedActorInfo.getInterface)

    val typedActor = createTypedActor(actorInfo, session.channel)
    val args = MessageSerializer.deserialize(request.getMessage).asInstanceOf[Array[AnyRef]].toList
    val argClasses = args.map(_.getClass)

    try {
      val messageReceiver = typedActor.getClass.getDeclaredMethod(typedActorInfo.getMethod, argClasses: _*)
      if (request.getOneWay) messageReceiver.invoke(typedActor, args: _*)
      else {
        //Sends the response
        def sendResponse(result: Either[Any, Throwable]): Unit = try {
          val messageBuilder = RemoteActorSerialization.createRemoteMessageProtocolBuilder(
            None,
            Right(request.getUuid),
            actorInfo.getId,
            actorInfo.getTarget,
            actorInfo.getTimeout,
            result,
            true,
            None,
            None,
            AkkaActorType.TypedActor,
            None)
          if (request.hasSupervisorUuid) messageBuilder.setSupervisorUuid(request.getSupervisorUuid)

          session.write(messageBuilder.build)
          log.slf4j.debug("Returning result from remote typed actor invocation [{}]", result)
        } catch {
          case e: Throwable => server.notifyListeners(RemoteServerError(e, server))
        }

        messageReceiver.invoke(typedActor, args: _*) match {
          case f: Future[_] => //If it's a future, we can lift on that to defer the send to when the future is completed
            f.onComplete(future => {
              val result: Either[Any, Throwable] =
                if (future.exception.isDefined) Right(future.exception.get) else Left(future.result.get)
              sendResponse(result)
            })
          case other => sendResponse(Left(other))
        }
      }
    } catch {
      case e: InvocationTargetException =>
        session.write(createErrorReplyMessage(e.getCause, request, AkkaActorType.TypedActor))
        server.notifyListeners(RemoteServerError(e, server))
      case e: Throwable =>
        session.write(createErrorReplyMessage(e, request, AkkaActorType.TypedActor))
        server.notifyListeners(RemoteServerError(e, server))
    }
  }


  /**
   * gets the actor from the session, or creates one if there is a factory for it
   */
  private def createSessionActor(actorInfo: ActorInfoProtocol, channel: T): ActorRef = {
    val uuid = actorInfo.getUuid
    val id = actorInfo.getId

    findSessionActor(id, channel) match {
      case null => // we dont have it in the session either, see if we have a factory for it
        server.findActorFactory(id) match {
          case null => null
          case factory =>
            val actorRef = factory()
            actorRef.uuid = parseUuid(uuid) //FIXME is this sensible?
            getSessionActors(channel).put(id, actorRef)
            actorRef
        }
      case sessionActor => sessionActor
    }
  }


  private def createClientManagedActor(actorInfo: ActorInfoProtocol): ActorRef = {
    val uuid = actorInfo.getUuid
    val id = actorInfo.getId
    val timeout = actorInfo.getTimeout
    val name = actorInfo.getTarget

    try {
      if (UNTRUSTED_MODE) throw new SecurityException(
        "Remote server is operating is untrusted mode, can not create remote actors on behalf of the remote client")

      log.slf4j.info("Creating a new client-managed remote actor [{}:{}]", name, uuid)
      val clazz = if (applicationLoader.isDefined) applicationLoader.get.loadClass(name)
      else Class.forName(name)
      val actorRef = Actor.actorOf(clazz.asInstanceOf[Class[_ <: Actor]])
      actorRef.uuid = parseUuid(uuid)
      actorRef.id = id
      actorRef.timeout = timeout
      server.actorsByUuid.put(actorRef.uuid.toString, actorRef) // register by uuid
      actorRef
    } catch {
      case e =>
        log.slf4j.error("Could not create remote actor instance", e)
        server.notifyListeners(RemoteServerError(e, server))
        throw e
    }

  }

  /**
   * Creates a new instance of the actor with name, uuid and timeout specified as arguments.
   *
   * If actor already created then just return it from the registry.
   *
   * Does not start the actor.
   */
  private def createActor(actorInfo: ActorInfoProtocol, channel: T): ActorRef = {
    val uuid = actorInfo.getUuid
    val id = actorInfo.getId

    server.findActorByIdOrUuid(id, parseUuid(uuid).toString) match {
      case null => // the actor has not been registered globally. See if we have it in the session
        createSessionActor(actorInfo, channel) match {
          case null => createClientManagedActor(actorInfo) // maybe it is a client managed actor
          case sessionActor => sessionActor
        }
      case actorRef => actorRef
    }
  }

  /**
   * gets the actor from the session, or creates one if there is a factory for it
   */
  private def createTypedSessionActor(actorInfo: ActorInfoProtocol, channel: T): AnyRef = {
    val id = actorInfo.getId
    findTypedSessionActor(id, channel) match {
      case null =>
        server.findTypedActorFactory(id) match {
          case null => null
          case factory =>
            val newInstance = factory()
            getTypedSessionActors(channel).put(id, newInstance)
            newInstance
        }
      case sessionActor => sessionActor
    }
  }

  private def createClientManagedTypedActor(actorInfo: ActorInfoProtocol) = {
    val typedActorInfo = actorInfo.getTypedActorInfo
    val interfaceClassname = typedActorInfo.getInterface
    val targetClassname = actorInfo.getTarget
    val uuid = actorInfo.getUuid

    try {
      if (UNTRUSTED_MODE) throw new SecurityException(
        "Remote server is operating is untrusted mode, can not create remote actors on behalf of the remote client")

      log.slf4j.info("Creating a new remote typed actor:\n\t[{} :: {}]", interfaceClassname, targetClassname)

      val (interfaceClass, targetClass) =
        if (applicationLoader.isDefined) (applicationLoader.get.loadClass(interfaceClassname),
            applicationLoader.get.loadClass(targetClassname))
        else (Class.forName(interfaceClassname), Class.forName(targetClassname))

      val newInstance = TypedActor.newInstance(
        interfaceClass, targetClass.asInstanceOf[Class[_ <: TypedActor]], actorInfo.getTimeout).asInstanceOf[AnyRef]
      server.typedActors.put(parseUuid(uuid).toString, newInstance) // register by uuid
      newInstance
    } catch {
      case e =>
        log.slf4j.error("Could not create remote typed actor instance", e)
        server.notifyListeners(RemoteServerError(e, server))
        throw e
    }
  }

  private[akka] def createTypedActor(actorInfo: ActorInfoProtocol, channel: T): AnyRef = {
    val uuid = actorInfo.getUuid

    server.findTypedActorByIdOrUuid(actorInfo.getId, parseUuid(uuid).toString) match {
      case null => // the actor has not been registered globally. See if we have it in the session
        createTypedSessionActor(actorInfo, channel) match {
          case null => createClientManagedTypedActor(actorInfo) //Maybe client managed actor?
          case sessionActor => sessionActor
        }
      case typedActor => typedActor
    }
  }


  private[akka] def createErrorReplyMessage(exception: Throwable, request: RemoteMessageProtocol, actorType: AkkaActorType): RemoteMessageProtocol = {
    val actorInfo = request.getActorInfo
    log.slf4j.error("Could not invoke remote actor [{}]", actorInfo.getTarget)
    log.slf4j.debug("Could not invoke remote actor", exception)
    val messageBuilder = RemoteActorSerialization.createRemoteMessageProtocolBuilder(
      None,
      Right(request.getUuid),
      actorInfo.getId,
      actorInfo.getTarget,
      actorInfo.getTimeout,
      Right(exception),
      true,
      None,
      None,
      actorType,
      None)
    if (request.hasSupervisorUuid) messageBuilder.setSupervisorUuid(request.getSupervisorUuid)
    messageBuilder.build
  }

  private[akka] def parseUuid(protocol: UuidProtocol): Uuid = uuidFrom(protocol.getHigh, protocol.getLow)

  protected[akka] def registerFilters(sendFilter: Pipeline.Filter, receiveFilter: Pipeline.Filter): Unit = {
    this.sendFilter = sendFilter
    this.receiveFilter = receiveFilter
  }

  protected[akka] def unregisterFilters(): Unit = {
    sendFilter = fallback
    receiveFilter = fallback
  }
}
