/**
 * Copyright (C) 2009-2010 Scalable Solutions AB <http://scalablesolutions.se>
 */

package akka.remote

import protocol.RemoteProtocol.RemoteMessageProtocol
import akka.remoteinterface.RemoteSupport
import akka.actor.Actor
import akka.util.Address

/**
 * Pipeline object to register and unregister client and server side filters.
 */
object Pipeline {
  type Filter = PartialFunction[Option[RemoteMessageProtocol], Option[RemoteMessageProtocol]]
  def identity:Filter = {case m => m}

  /**
   * Registers filters on the client side of the remote protocol. The filter is registered on the connection with the remoteAddress.
   * The client sendFilter is the PartialFunction that the remote message is passed through before sending from the client side.
   * The client receiveFilter is the PartialFunction that the remote message is passed through before receiving on the client side.
   */
  def registerClientFilters(remoteAddress: Address, sendFilter: Filter, receiveFilter:Filter=identity, remoteSupport: RemoteSupport = Actor.remote): Unit = {
    // it is asInstanceOf, or move protobuf RemoteProtocol classes to akka-actor
    val clientFilters = remoteSupport.asInstanceOf[ClientFilters]
    clientFilters.registerClientFilters(sendFilter,receiveFilter, remoteAddress)
  }

  /**
   * Unregisters the existing send and receive filter on the client side for the remoteAddress
   */
  def unregisterClientFilters(remoteAddress: Address, remoteSupport: RemoteSupport = Actor.remote): Unit = {
    val clientFilters = remoteSupport.asInstanceOf[ClientFilters]
    clientFilters.unregisterClientFilters(remoteAddress)
  }

  /**
   * Registers filters on the server side of the remote protocol. The filter is registered on the address of the remote server.
   * The server sendFilter is the PartialFunction that the remote message is passed through before sending back to the client.
   * The server receiveFilter is the PartialFunction that the remote message is passed through before receiving the remote message at the server
   * and passing it on to the actor.
   */
  def registerServerFilters(address: Address, sendFilter: Filter, receiveFilter:Filter=identity, remoteSupport: RemoteSupport = Actor.remote): Unit = {
    val serverFilters = remoteSupport.asInstanceOf[ServerFilters]
    serverFilters.registerServerFilters(sendFilter,receiveFilter)
  }
  def unregisterServerFilters(address: Address, remoteSupport: RemoteSupport = Actor.remote): Unit = {
    val serverFilters = remoteSupport.asInstanceOf[ServerFilters]
    serverFilters.unregisterServerFilters
  }
}