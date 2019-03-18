package cloud.fchen.spark.utils

import java.io.File
import java.net.{URI, URLClassLoader}
import java.nio.file.{Files, Paths}

import scala.collection.JavaConverters._

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.internal.Logging

class IdeaUtil(
    val hadoopConfDir: Option[String],
    val classpathTempDir: Option[String],
    val dependenciesInHDFSPath: String,
    val principal: Option[String],
    val keytab: Option[String]) extends Logging {

  val HADOOP_CONF_DIR = "file:///Users/fchen/tmp/hadoop"

  def setup(): Map[String, String] = {
    copyHdfsConfToClasspath(Option(HADOOP_CONF_DIR))
    login()
    copyUserClasses
    val jars = uploadUserJar(dependenciesInHDFSPath)
    Map(
      "spark.repl.class.outputDir" -> classpathTempDir.get,
      "spark.jars" -> jars.mkString(","),
      "spark.master" -> "yarn-client"
    )
  }

  def login(): Unit = {
    if (principal.isDefined && keytab.isDefined) {
      logInfo(s"logining for user ${principal.get} using keytab file ${keytab.get}")
      // 需要手动login，原因是因为提交给yarn的时候需要kerberos认证
      UserGroupInformation.loginUserFromKeytab(principal.get, keytab.get)
    }
  }

  def copyUserClasses: Unit = {
    val target = Paths.get(new File(classpathTempDir.get).toURI)
    if (Files.isDirectory(target)) {
      logInfo(s"delete path ${classpathTempDir.get}")
      target.toFile.delete()
    }
    val classLoader = ClassLoader.getSystemClassLoader()
    val urls = classLoader.asInstanceOf[URLClassLoader].getURLs
    urls.foreach(u => {
      if (u.getFile.contains("target")) {
        logInfo(s"copy file ${u.toString}")
        val source = Paths.get(new URI(u.toString))
        //        Files.copy(source, target)
        FileUtils.copyDirectory(source.toFile, target.toFile)
      }
    })

  }

  def uploadUserJar(appHdfsPath: String): Array[String] = {

    val javaHome = System.getenv("JAVA_HOME")
    val classLoader = ClassLoader.getSystemClassLoader()
    val urls = classLoader.asInstanceOf[URLClassLoader].getURLs
    val userJars = urls.filter(
      _.getFile.endsWith("jar")
    ).filter(
      !_.getFile.startsWith(javaHome)
    ).filter(
      !_.getFile.endsWith("idea_rt.jar")
    )
      .map(u => {
        Paths.get(u.getFile)
      })

    val fs = FileSystem.get(configuration(Option(HADOOP_CONF_DIR)))
    createDir(fs, appHdfsPath)
    //    val toUpload = ArrayBuffer[JPath]()

    val jarInHdfs = fs.listStatus(new Path(appHdfsPath))
      .map(s => {
        s.getPath.getName
      })

    userJars.foreach(j => {
      if (!jarInHdfs.contains(j.getFileName.toString)) {
        // upload jar file
        copyFileFromLocal(fs, j.toString, appHdfsPath)
      }
    })

    // show jar file in hdfs.
    listUserJarFile(fs, appHdfsPath)
  }

  def listUserJarFile(fs: FileSystem, path: String): Array[String] = {
    fs.listStatus(new Path(path))
      .map(s => {
//        println(s.getPath)
        s.getPath.toString
      })
  }

  def createDir(fs: FileSystem, hdfsDir: String): Unit = {
    val path = new Path(hdfsDir)
    if (!fs.exists(path)) {
      fs.mkdirs(path)
    }
  }

  def copyFileFromLocal(fs: FileSystem, source: String, targetDir: String): Unit = {
    fs.copyFromLocalFile(new Path(source), new Path(targetDir))
  }

  def getHDFSConf: Map[String, String] = {
    val conf = configuration(Option(HADOOP_CONF_DIR))
    conf.asScala.map(x => {
      (x.getKey, x.getValue)
    }).toMap
  }

  def copyHdfsConfToClasspath(hadoopConfDir: Option[String]): Unit = {

    val confDir = hadoopConfDir.getOrElse(System.getenv("HADOOP_CONF_DIR"))
    val classLoader = ClassLoader.getSystemClassLoader()
    val url = classLoader.asInstanceOf[URLClassLoader].getURLs.filter(_.getFile.contains("target")).head

    // source directory
    val source = Paths.get(new URI(confDir))
    // target directory
    val target = Paths.get(new URI(url.toString))
    FileUtils.copyDirectory(source.toFile, target.toFile)
  }

  def configuration(hadoopConfDir: Option[String]): Configuration = {
    //    val confDir = hadoopConfDir.getOrElse(System.getenv("HADOOP_CONF_DIR"))
    //    val conf = new Configuration()
    //    println(new File(confDir + "/yarn-site.xml").toURI.toURL)
    //    conf.addResource(new File(confDir + "/yarn-site.xml").toURI.toURL)
    //    conf.addResource(new File(confDir + "/hdfs-site.xml").toURI.toURL)
    //    conf.addResource(new File(confDir + "/core-site.xml").toURI.toURL)
    //    conf.addResource(new File(confDir + "/ssl-client.xml").toURI.toURL)
    //    conf.asScala.map(x => {
    //      println(x)
    //      (x.getKey, x.getValue)
    //    }).toMap
    //    conf
    new Configuration()
  }

}

trait HDFSOperations {
  def login(principal: Option[String], keytab: Option[String]): Unit = {
    if (principal.isDefined && keytab.isDefined) {
      // scalastyle:off println
      println(s"logining for user ${principal.get} using keytab file ${keytab.get}")
      // scalastyle:on
      // 需要手动login，原因是因为提交给yarn的时候需要kerberos认证
      UserGroupInformation.loginUserFromKeytab(principal.get, keytab.get)
    }
  }
}
