import org.jenkinsci.plugins.pipeline.modeldefinition.Utils

def buildNumber = env.BUILD_NUMBER as int
if (buildNumber > 1)
  milestone(buildNumber - 1)
milestone(buildNumber)

FULL_CI = buildWithFullCi()
if (FULL_CI) {
  githubNotify context: 'full-ci', status: 'SUCCESS'
}

pipeline {
  agent none

  stages {
    stage("Amazon Linux") {
      agent {
        docker {
          image 'hpiepic/skyrise:build'
          alwaysPull true
          args '--dns=192.168.30.50'
        }
      }
      environment {
        AWS_ACCESS_KEY_ID = credentials('skyrise-ci-aws-access-key-id')
        AWS_SECRET_ACCESS_KEY = credentials('skyrise-ci-aws-secret-access-key')
        CCACHE_DISABLE = 'true'
        JENKINS_HTTPS_AUTH = credentials('skyrise-ci-https-auth')
      }
      steps {
        script {
          parallel(
            "ClangDebug": {
              stage("ClangDebug") {
                stage("Build") {
                  sh 'mkdir cmake-build-debug'
                  dir('cmake-build-debug') {
                    sh '''cmake .. -GNinja \
                        -DCMAKE_C_COMPILER=/usr/bin/clang \
                        -DCMAKE_CXX_COMPILER=/usr/bin/clang++ \
                        -DCMAKE_BUILD_TYPE=Debug \
                        -DSKYRISE_ENABLE_CCACHE=OFF \
                        -DSKYRISE_ENABLE_CLANG_TIDY=ON \
                        -DSKYRISE_ENABLE_LLVM_COV=ON'''
                    sh 'ninja-build all -j$(nproc)'
                  }
                }
                stage("Test") {
                  dir('cmake-build-debug') {
                    sh 'LLVM_PROFILE_FILE=skyriseTest.profraw bin/skyriseTest --gtest_output="xml:test-results.xml"'
                  }
                  xunit(
                    thresholds: [
                      skipped(failureThreshold: '0'),
                      failed(failureThreshold: '0')
                    ],
                    tools: [
                      GoogleTest(pattern: 'cmake-build-debug/test-results.xml')
                    ]
                  )
                }
                stage("Coverage") {
                  dir('cmake-build-debug') {
                    sh '''llvm-profdata merge -sparse skyriseTest.profraw -o skyriseTest.profdata &&
                        llvm-cov show -format=html -ignore-filename-regex="(third_party|test)" -output-dir=coverage \
                        -instr-profile=skyriseTest.profdata bin/skyriseTest'''
                    
                    publishHTML([
                      allowMissing: false,
                      alwaysLinkToLastBuild: false,
                      keepAll: true,
                      reportDir: 'coverage',
                      reportFiles: 'index.html',
                      reportName: 'LLVM-Coverage-Report',
                      reportTitles: ''
                    ])
                    
                    sh 'llvm-cov report -summary-only -ignore-filename-regex="(third_party|test)" \
                        -instr-profile=skyriseTest.profdata bin/skyriseTest | tail -n1 -c7 > coverage_percentage.txt'
                    archiveArtifacts 'coverage_percentage.txt'

                    output = sh script: '../script/compare_coverage.sh', returnStdout: true
                    (coverage_status, coverage_message) = output.trim().tokenize(';')
                    githubNotify context: 'llvm-cov', description: "$coverage_message", status: "$coverage_status", \
                      targetUrl: "${env.BUILD_URL}LLVM-Coverage-Report/index.html"
                  }
                }
              }
            },
            "ClangRelease": {
              stage("ClangRelease") {
                stage("Build") {
                  if (FULL_CI == true) {
                    sh 'mkdir cmake-build-release'
                    dir('cmake-build-release') {
                      sh '''cmake .. -GNinja \
                          -DCMAKE_C_COMPILER=/usr/bin/clang \
                          -DCMAKE_CXX_COMPILER=/usr/bin/clang++ \
                          -DCMAKE_BUILD_TYPE=Release \
                          -DSKYRISE_ENABLE_CCACHE=OFF'''
                      sh 'ninja-build all -j$(nproc)'
                    }
                  } else {
                    Utils.markStageSkippedForConditional("ClangRelease")
                  }
                }
              }
            },
            "ClangFormat": {
              stage("ClangFormat") {
                stage("Format") {
                  sh 'python3 script/run_clang_format.py --clang_format_binary clang-format --source_dir src --quiet'
                }
              }
            }
          )
        }
      }
    }
    stage("Ubuntu") {
      when {
        beforeAgent true
        expression { FULL_CI == true }
      }
      agent {
        docker {
          image 'hpiepic/skyrise:ubuntu'
          alwaysPull true
        }
      }
      steps {
        script {
          parallel(
            "GccDebug": {
              stage("GccDebug") {
                stage("Build") {
                  sh 'mkdir cmake-build-debug'
                  dir('cmake-build-debug') {
                    sh '''cmake .. -GNinja \
                        -DCMAKE_C_COMPILER=/usr/bin/gcc \
                        -DCMAKE_CXX_COMPILER=/usr/bin/g++ \
                        -DCMAKE_BUILD_TYPE=Debug \
                        -DSKYRISE_ENABLE_CCACHE=OFF'''
                    sh 'ninja all -j$(nproc)'
                  }
                }
              }
            }
          )
        }
      }
    }
  }
  post {
    changed {
      node(null) {
        script {
          isSuccess = currentBuild.currentResult == 'SUCCESS'
          slackSend(
            channel: '#ci',
            color: isSuccess ? '#5cb58a' : '#FF0000',
            message: """\
            *[${currentBuild.currentResult}] <${env.RUN_DISPLAY_URL}|Build #${env.BUILD_NUMBER}>*\
${buildWithFullCi() ? ' (with full-ci label)' : ''}
            ${getChangeType()}: <${getChangeUrl()}|${getChangeName()}>
            Commit: ${getCommitMessage()} (<${getCommitUrl()}|${getCommitSha().substring(0, 7)}>)
            Author: ${getSlackAuthorMention()}${isSuccess ? '' : ' (also looping in @channel)'}
            """.stripIndent()
          )
        }
      }
    }
  }
}

Boolean buildWithFullCi() {
  if (env.CHANGE_ID) {
    return pullRequest.labels.contains('full-ci')
  }
  return env.BRANCH_NAME == 'master'
}

String getSlackAuthorMention() {
  committerSlackUserId = getCommitterSlackUserId()
  if (committerSlackUserId == null)
    return ""
  return "<@${committerSlackUserId}>"
}

String getCommitterSlackUserId() {
  if (env.CHANGE_ID)
    branchName = pullRequest.headRef
  else
    branchName = env.BRANCH_NAME

  if (branchName == "master")
    return null

  endOfBranchPrefix = branchName.indexOf('/')
  if (endOfBranchPrefix == -1)
    return "Unknown (Invalid branch name: ${branchName})"

  committerName = branchName.substring(0, endOfBranchPrefix).toLowerCase()
  gitHubToSlack = [
    "cajan93": "U01435GN1U5",
    "d-justen": "U0144J0QCPM",
    "engelfa": "U014UBW46AU",
    "jansiebert": "U0142D6U51T",
    "jkhlr": "U0149F0BZPW",
    "julianmenzler": "U01FTU7BEP3",
    "maltenbergert": "U014GG68EDP",
    "mweisgut": "U01G1P36TEF",
    "pscls": "U01DC9KJJ0N",
    "tobodner": "U014FR9CNRF"
  ]
  return gitHubToSlack.containsKey(committerName) ?
          gitHubToSlack[committerName] : "Unknown (Invalid branch prefix: ${committerName})"
}

String getChangeType() {
  if (env.CHANGE_ID)
    return "Pull Request"
  return "Branch"
}

String getChangeName() {
  if (env.CHANGE_ID)
    return "${pullRequest.title} (#${pullRequest.number})"
  return env.BRANCH_NAME
}

String getChangeUrl() {
  if (env.CHANGE_ID)
    return "${getRepoUrl()}/pull/${env.CHANGE_ID}"
  return "${getRepoUrl()}/tree/${env.BRANCH_NAME}"
}

String getCommitUrl() {
  return "${getRepoUrl()}/commit/${getCommitSha()}"
}

String getCommitMessage() {
  if (env.CHANGE_ID)
    return getShellOutput("git --no-pager show HEAD^ -s --format=%s")
  return getShellOutput("git --no-pager show -s --format=%s")
}

String getCommitSha() {
  if (env.CHANGE_ID)
    return getShellOutput("git rev-parse HEAD^")
  return getShellOutput("git rev-parse HEAD")
}

String getRepoUrl() {
  return scm.getUserRemoteConfigs()[0].getUrl()
            .substring(0, scm.getUserRemoteConfigs()[0].getUrl().lastIndexOf('.'))
}

String getShellOutput(command) {
  return sh(script: '#!/bin/sh -e\n' + command, returnStdout: true).trim()
}
