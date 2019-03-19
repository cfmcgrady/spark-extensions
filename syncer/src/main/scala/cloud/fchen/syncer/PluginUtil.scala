package cloud.fchen.syncer

import java.io.File

import scala.collection.JavaConverters._
import scala.collection.mutable.HashSet

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.security.UserGroupInformation
import org.apache.maven.execution.MavenSession
import org.apache.maven.plugin.AbstractMojo
import org.apache.maven.project.MavenProject

/**
 * @time 2019-03-18 12:40
 * @author fchen <cloud.chenfu@gmail.com>
 */
abstract class PluginUtil extends AbstractMojo with HDFSOperations {

  def run(session: MavenSession,
           project: MavenProject,
           archivePath: String,
           appHdfsPath: String,
           principal: String,
           keytab: String): Unit = {

    val confDir = sys.env.getOrElse("HADOOP_CONF_DIR", {
      throw new RuntimeException("maven syncer plugin require HADOOP_CONF_DIR environment variable.")
    })
//    val buildReq = new DefaultProjectBuildingRequest(session.getProjectBuildingRequest)

    val conf = configuration(Option(confDir), Option(principal), Option(keytab))

    val fs = FileSystem.get(conf)
    createDir(fs, appHdfsPath)
    val jarsInHDFS = showJarInHDFS(fs, appHdfsPath)
    val currentPath = new Path(appHdfsPath)

    getLog.info(s"uploading project dependencies using directory [ ${currentPath.toUri.toString} ].")
    val dependencies = project.getArtifacts.asScala.map(a => {
      if (!jarsInHDFS.contains(a.getFile.getName)) {
        getLog.info(s"uploading ${a.getFile.getName} ...")
        copyFileFromLocal(fs, a.getFile.getPath, appHdfsPath)
      }
      a.getFile.getName
    })

    if (!archivePath.endsWith(".pom")) {
      getLog.info(s"uploading project jar file ${archivePath}.")
      delete(fs, archivePath)
      copyFileFromLocal(fs, archivePath, appHdfsPath)
    }

    val cacheJars = Cache.add(dependencies.toSet)
    val removedJars = jarsInHDFS.diff(cacheJars.toArray[String])
    removedJars.foreach(jar => {
      getLog.info(s"removing dependency ${jar}.")
    })
  }
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
      println(s"logining for user ${principal.get} using keytab file ${keytab.get}")
      // scalastyle:on
      // 需要手动login，原因是因为提交给yarn的时候需要kerberos认证
      conf.set("hadoop.security.authentication", "Kerberos")
      UserGroupInformation.setConfiguration(conf)
      UserGroupInformation.loginUserFromKeytab(principal.get, keytab.get)
    }
    conf
  }

}

object Cache {

  private val _cache = HashSet.empty[String]

  def add(jars: Set[String]): Set[String] = {
    _cache ++= jars
    _cache.toSet
  }

}
