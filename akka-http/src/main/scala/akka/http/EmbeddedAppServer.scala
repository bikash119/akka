/**
 * Copyright (C) 2009-2011 Scalable Solutions AB <http://scalablesolutions.se>
 */

package akka.http

import javax.ws.rs.core.UriBuilder
import javax.servlet.ServletConfig
import java.io.File

import akka.actor.BootableActorLoaderService
import akka.util.{Bootable, Logging}

import org.eclipse.jetty.xml.XmlConfiguration
import org.eclipse.jetty.server.{Handler, Server}
import org.eclipse.jetty.server.handler.{HandlerList, HandlerCollection, ContextHandler}
import java.net.URL
import akka.AkkaException

/**
 * Handles the Akka Comet Support (load/unload)
 */
trait EmbeddedAppServer extends Bootable with Logging {
  self: BootableActorLoaderService =>

  import akka.config.Config._

  val REST_HOSTNAME = config.getString("akka.http.hostname", "localhost")
  val REST_PORT = config.getInt("akka.http.port", 9998)

  val isRestEnabled = config.getList("akka.enabled-modules").exists(_ == "http")

  protected var server: Option[Server] = None

  protected def findJettyConfigXML: Option[URL] =
    Option(applicationLoader.getOrElse(this.getClass.getClassLoader).getResource("microkernel-server.xml")) orElse
    HOME.map(home => new File(home + "/config/microkernel-server.xml").toURI.toURL)

  abstract override def onLoad = {
    super.onLoad
    if (isRestEnabled) {
      log.slf4j.info("Attempting to start Akka HTTP service")

      val configuration = new XmlConfiguration(findJettyConfigXML.getOrElse(error("microkernel-server.xml not found!")))

      System.setProperty("jetty.port", REST_PORT.toString)
      System.setProperty("jetty.host", REST_HOSTNAME)

      HOME.foreach( home => System.setProperty("jetty.home", home + "/deploy/root") )

      server = Option(configuration.configure.asInstanceOf[Server]) map { s => //Set the correct classloader to our contexts
         applicationLoader foreach { loader =>
           //We need to provide the correct classloader to the servlets
           def setClassLoader(handlers: Seq[Handler]): Unit = {
             handlers foreach {
               case c: ContextHandler    => c.setClassLoader(loader)
               case c: HandlerCollection => setClassLoader(c.getHandlers)
               case _ =>
             }
           }
           setClassLoader(s.getHandlers)
         }
         //Start the server
         s.start()
         s
      }
      log.slf4j.info("Akka HTTP service started")
    }
  }

  abstract override def onUnload = {
    super.onUnload
    server foreach { t =>
      log.slf4j.info("Shutting down REST service (Jersey)")
      t.stop()
    }
  }
}
