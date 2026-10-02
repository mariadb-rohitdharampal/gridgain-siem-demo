$ErrorActionPreference = "Stop"

$moduleArgs = @(
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
    "--add-opens=java.base/java.io=ALL-UNNAMED",
    "--add-opens=java.base/java.net=ALL-UNNAMED",
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/java.util=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent.locks=ALL-UNNAMED",
    "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens=java.management/com.sun.jmx.mbeanserver=ALL-UNNAMED",
    "--add-opens=jdk.internal.jvmstat/sun.jvmstat.monitor=ALL-UNNAMED",
    "--add-exports=java.base/jdk.internal.misc=ALL-UNNAMED",
    "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
)

$env:MAVEN_OPTS = $moduleArgs -join " "

mvn -q package "-DskipTests"
mvn -q dependency:build-classpath "-DincludeScope=runtime" "-Dmdep.outputFile=target/runtime-classpath.txt"

$runtimeClasspath = (Get-Content target/runtime-classpath.txt -Raw).Trim()
$demoArgs = @("--events", "10000", "--target-reduction", "40", "--reducer", "gridgain", "--ignite-nodes", "3") + $args
& java @moduleArgs -cp "target/classes;$runtimeClasspath" com.gridgain.demo.siem.Main @demoArgs
