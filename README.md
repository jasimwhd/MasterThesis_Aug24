<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>org.mavenhbase.example</groupId>
    <artifactId>mavenhbasetest</artifactId>
    <version>1.0-SNAPSHOT</version>
    <properties>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    </properties>

    <dependencies>
            <dependency>
                <groupId>org.apache.hadoop</groupId>
                <artifactId>hadoop-common</artifactId>
                <version>2.7.3</version>
                <exclusions>

                    <exclusion>
                        <groupId>javax.servlet</groupId>
                        <artifactId>*</artifactId>
                    </exclusion>

                </exclusions>

            </dependency>
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-client</artifactId>
            <version>2.7.3</version>
        </dependency>
        <dependency>
                 <groupId>org.apache.spark</groupId>
                 <artifactId>spark-core_2.10</artifactId>
                 <version>1.6.3</version>

                 <exclusions>
                     <exclusion>
                         <groupId>org.apache.hadoop</groupId>
                         <artifactId>hadoop-client</artifactId>
                     </exclusion>
              <exclusion>
                  <groupId>org.eclipse.jetty.orbit</groupId>
                  <artifactId>javax.servlet</artifactId>
              </exclusion>
                 </exclusions>
             </dependency>
             <!-- https://mvnrepository.com/artifact/org.apache.spark/spark-sql_2.10 -->
             <dependency>
                 <groupId>org.apache.spark</groupId>
                 <artifactId>spark-sql_2.10</artifactId>
                 <version>1.6.3</version>

             </dependency>
        <!-- https://mvnrepository.com/artifact/org.spark-project.hive/hive-metastore -->

        <!-- https://mvnrepository.com/artifact/org.apache.spark/spark-hive_2.10 -->
         <dependency>
             <groupId>org.apache.spark</groupId>
             <artifactId>spark-hive_2.10</artifactId>
             <version>1.6.3</version>

         </dependency>
        <dependency>
            <groupId>com.fasterxml.jackson.core</groupId>
            <artifactId>jackson-databind</artifactId>
            <version>2.4.4</version>
        </dependency>
        <!-- https://mvnrepository.com/artifact/com.fasterxml.jackson.core/jackson-core -->
        <dependency>
            <groupId>com.fasterxml.jackson.core</groupId>
            <artifactId>jackson-core</artifactId>
            <version>2.4.4</version>
        </dependency>
        <!-- https://mvnrepository.com/artifact/com.fasterxml.jackson.core/jackson-annotations -->
        <dependency>
            <groupId>com.fasterxml.jackson.core</groupId>
            <artifactId>jackson-annotations</artifactId>
            <version>2.4.4</version>
        </dependency>
        <!-- https://mvnrepository.com/artifact/javax.servlet/javax.servlet-api -->
        <dependency>
            <groupId>javax.servlet</groupId>
            <artifactId>javax.servlet-api</artifactId>
            <version>3.1.0</version>
            <scope>provided</scope>
        </dependency>


    </dependencies>
    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <version>2.0.2</version>
                <configuration>
                    <source>1.8</source>
                    <target>1.8</target>
                </configuration>
            </plugin>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-jar-plugin</artifactId>
                <configuration>
                    <archive>
                        <manifest>
                            <addClasspath>true</addClasspath>
                            <classpathPrefix>lib/</classpathPrefix>
                            <mainClass>hbasepingtest.MavenMainHbase</mainClass>
                        </manifest>
                    </archive>
                </configuration>
            </plugin>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-shade-plugin</artifactId>
                <executions>
                    <execution>
                        <id>shade</id>
                        <goals>
                            <goal>shade</goal>
                        </goals>
                        <configuration>
                            <shadedArtifactAttached>true</shadedArtifactAttached>
                            <transformers>
                                <transformer
                                        implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                                    <mainClass>hbasepingtest.MavenMainHbase</mainClass>
                                </transformer>
                            </transformers>
                        </configuration>

                    </execution>
                </executions>
            </plugin>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-dependency-plugin</artifactId>
                <executions>
                    <execution>
                        <id>copy</id>
                        <phase>install</phase>
                        <goals>
                            <goal>copy-dependencies</goal>
                        </goals>
                        <configuration>
                            <outputDirectory>${project.build.directory}/lib</outputDirectory>
                        </configuration>

                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>


</project>
