package cloud.fchen.syncer

import java.io.File
import java.nio.file.Files
import java.security.{DigestInputStream, MessageDigest}

import scala.collection.JavaConverters._
import scala.collection.mutable.HashSet

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.security.UserGroupInformation
import org.apache.maven.artifact.Artifact
import org.apache.maven.execution.MavenSession
import org.apache.maven.plugin.AbstractMojo
import org.apache.maven.project.MavenProject

/**
 * @time 2019-03-18 12:40
 * @author fchen <cloud.chenfu@gmail.com>
 */
abstract class PluginUtil extends AbstractMojo with HDFSOperations {

  override def logInfo(log: String): Unit = getLog.info(log)

  def run(session: MavenSession,
          project: MavenProject,
          archivePath: String,
          appHdfsPath: String,
          principal: String,
          keytab: String): Unit = {

    val confDir = sys.env.getOrElse("HADOOP_CONF_DIR", {
      throw new RuntimeException("maven syncer plugin require HADOOP_CONF_DIR environment variable.")
    })

    val conf = configuration(Option(confDir), Option(principal), Option(keytab))

    val fs = FileSystem.get(conf)
    createDir(fs, appHdfsPath)
    val jarsInHDFS = showJarInHDFS(fs, appHdfsPath)
    val currentPath = new Path(appHdfsPath)

    // first upload the project dependencies jar file.
    getLog.info(s"uploading project dependencies using directory [ ${currentPath.toUri.toString} ].")
    project.getArtifacts.asScala.map(artifact => {
      withCache(artifact) {
        if (!jarsInHDFS.contains(artifact.getFile.getName)) {
          getLog.info(s"uploading ${artifact.getFile.getName} ...")
          copyFileFromLocal(fs, artifact.getFile.getPath, appHdfsPath)
        }
      }
    })

    // next, upload project artifact jar file.

    // make sure the project jar file is not found in hdfs.
    delete(fs, project.getArtifact.getFile.getName)
    // upload the project jar file.
    withCache(project.getArtifact) {
      copyFileFromLocal(fs, project.getArtifact.getFile.getPath, appHdfsPath)
    }

    // clear the unnecessary dependency jar file.
    val removedJars = jarsInHDFS.diff(_cache.toArray[String])
    removedJars.foreach(jar => {
      val absolutePath = appHdfsPath + Path.SEPARATOR + jar
      getLog.info(s"removing dependency ${absolutePath}.")
      delete(fs, absolutePath)
    })
  }

  def sha1(file: File): String = {
    val sha = MessageDigest.getInstance("SHA-1")
    val dis = new DigestInputStream(Files.newInputStream(file.toPath), sha)
    while (dis.available > 0) {
      dis.read
    }
    dis.close
    sha.digest.map(b => String.format("%02x", Byte.box(b))).mkString
  }

  private def withCache[T](artifact: Artifact)(f: => T) {
    getLog.debug(s"add ${artifact.getFile.getName} to cache.")
    _cache += artifact.getFile.getName
    f
  }

  private lazy val _cache = HashSet.empty[String]

}

trait HDFSOperations {

  def showJarInHDFS(fs: FileSystem, appHdfsPath: String): Array[String] = {

    fs.listStatus(new Path(appHdfsPath))
      .map(s => {
        s.getPath.getName
      })

  }

  def copyFileFromLocal(fs: FileSystem, source: String, targetDir: String): Unit = {
    fs.copyFromLocalFile(new Path(source), new Path(targetDir))
  }

  def delete(fs: FileSystem, source: String): Unit = {
    fs.delete(new Path(source), true)
  }

  def createDir(fs: FileSystem, hdfsDir: String): Unit = {
    val path = new Path(hdfsDir)
    if (!fs.exists(path)) {
      fs.mkdirs(path)
    }
  }

//  def login(principal: Option[String], keytab: Option[String]): Unit = {
//    if (principal.isDefined && keytab.isDefined) {
//      // scalastyle:off println
//      println(s"logining for user ${principal.get} using keytab file ${keytab.get}")
//      // scalastyle:on
//      // 需要手动login，原因是因为提交给yarn的时候需要kerberos认证
//      UserGroupInformation.loginUserFromKeytab(principal.get, keytab.get)
//    }
//  }

  def configuration(hadoopConfDir: Option[String],
                    principal: Option[String],
                    keytab: Option[String]): Configuration = {
    val conf = new Configuration()
    if (hadoopConfDir.isDefined) {
      val addResource: (String) => Unit = {
        (s: String) => {
//          conf.addResource(new File(confDir + "/yarn-site.xml").toURI.toURL)
          val f = new File(hadoopConfDir.get + "/" + s)
          if (f.exists()) {
            conf.addResource(f.toURI.toURL)
          }
        }
      }
      addResource("core-site.xml")
      addResource("hdfs-site.xml")
      addResource("yarn-site.xml")
    }
    if (principal.isDefined && keytab.isDefined) {
      // scalastyle:off println
      logInfo(s"logining for user ${principal.get} using keytab file ${keytab.get}")
      // scalastyle:on
      // 需要手动login，原因是因为提交给yarn的时候需要kerberos认证
      conf.set("hadoop.security.authentication", "Kerberos")
      UserGroupInformation.setConfiguration(conf)
      UserGroupInformation.loginUserFromKeytab(principal.get, keytab.get)
    }
    conf
  }

  def logInfo(log: String): Unit

}
