package akka.remote

import protocol.RemoteProtocol.RemoteMessageProtocol
import akka.remoteinterface.RemoteSupport
import akka.actor.Actor
import akka.util.Address

object Pipeline {
  type Filter = PartialFunction[Option[RemoteMessageProtocol], Option[RemoteMessageProtocol]]
  def identity:Filter = {case m => m}

  def registerClientFilters(remoteAddress: Address, sendFilter: Filter, receiveFilter:Filter=identity, remoteSupport: RemoteSupport = Actor.remote): Unit = {
    // it is this, or move protobuf RemoteProtocol classes to akka-actor
    val clientFilters = remoteSupport.asInstanceOf[ClientFilters]
    clientFilters.registerClientFilters(sendFilter,receiveFilter, remoteAddress)
  }
  def unregisterClientFilters(remoteAddress: Address, remoteSupport: RemoteSupport = Actor.remote): Unit = {
    val clientFilters = remoteSupport.asInstanceOf[ClientFilters]
    clientFilters.unregisterClientFilters(remoteAddress)
  }

  def registerServerFilters(address: Address, sendFilter: Filter, receiveFilter:Filter=identity, remoteSupport: RemoteSupport = Actor.remote): Unit = {
    // it is this, or move protobuf RemoteProtocol classes to akka-actor
    val serverFilters = remoteSupport.asInstanceOf[ServerFilters]
    serverFilters.registerServerFilters(sendFilter,receiveFilter)
  }
  def unregisterServerFilters(address: Address, remoteSupport: RemoteSupport = Actor.remote): Unit = {
    val serverFilters = remoteSupport.asInstanceOf[ServerFilters]
    serverFilters.unregisterServerFilters
  }
}