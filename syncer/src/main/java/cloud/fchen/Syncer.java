package cloud.fchen;

import cloud.fchen.syncer.PluginTest;
import org.apache.maven.artifact.resolver.ArtifactResolver;
import org.apache.maven.execution.MavenSession;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.Component;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.plugins.annotations.ResolutionScope;
import org.apache.maven.project.MavenProject;
import org.apache.maven.project.ProjectBuilder;

/**
 * @author fchen <cloud.chenfu@gmail.com>
 * @time 2019-03-18 13:43
 */
@Mojo(name = "package", requiresDependencyResolution = ResolutionScope.RUNTIME)
public class Syncer extends PluginTest {
  @Parameter(defaultValue = "${project}", readonly = true, required = true)
  private MavenProject project;

  @Parameter(defaultValue = "${session}", readonly = true, required = true)
  private MavenSession session;


  @Parameter( property = "hdfsDirectory", required = true)
  private String hdfsDirectory;

  @Parameter( property = "principal")
  private String principal;

  @Parameter( property = "keytab")
  private String keytab;

  @Component
  private ProjectBuilder projectBuilder;

  @Parameter( property = "archivePath", defaultValue = "${project.build.directory}/${project.build.finalName}.${project.packaging}")
  private String archivePath;

  @Override public void execute() throws MojoExecutionException, MojoFailureException {
    try {
      run2(session, project, archivePath, hdfsDirectory, principal, keytab);
    } catch (Exception e) {
      getLog().error("error.", e);
    }
  }
}
