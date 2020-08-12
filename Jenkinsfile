pipeline {
    agent {
        docker {
            image 'hpiepic/skyrise:build'
            alwaysPull true
        }
    }

    stages {
        stage ("Parallel Pipeline"){
            environment {
                AWS_ACCESS_KEY_ID = credentials('skyrise-ci-aws-access-key-id')
                AWS_SECRET_ACCESS_KEY = credentials('skyrise-ci-aws-secret-access-key')
            }
            steps {
                script {
                    parallel(
                        "Format": {
                            stage ("Format") {
                                stage("clang-format") {
                                    sh 'python3 script/run_clang_format.py --clang_format_binary clang-format --source_dir src --quiet'
                                }
                            }
                        },
                        "ClangDebug": {
                            stage ("ClangDebug") {
                                stage("Build") {
                                    sh 'mkdir -p cmake-build-debug'
                                    dir('cmake-build-debug') {
                                        sh 'cmake .. -DCMAKE_C_COMPILER=/usr/bin/clang -DCMAKE_CXX_COMPILER=/usr/bin/clang++ -DCMAKE_BUILD_TYPE=Debug -DSKYRISE_ENABLE_CLANG_TIDY=ON'
                                        sh 'make all -j$(nproc)'
                                    }
                                }
                                stage("Test") {
                                    dir('cmake-build-debug') {
                                        sh 'bin/skyriseTest --gtest_output="xml:test-results.xml"'
                                    }
                                }
                            }
                        },
                        "ClangRelease": {
                            stage ("ClangRelease") {
                                stage("Build") {
                                    sh 'mkdir -p cmake-build-release'
                                    dir('cmake-build-release') {
                                        sh 'cmake .. -DCMAKE_C_COMPILER=/usr/bin/clang -DCMAKE_CXX_COMPILER=/usr/bin/clang++ -DCMAKE_BUILD_TYPE=Release'
                                        sh 'make all -j$(nproc)'
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
        always {
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
        changed {
            script {
                isSuccess = currentBuild.currentResult == 'SUCCESS'
                slackSend(
                     channel: '#ci',
                     color: isSuccess ? '#5cb58a' : '#FF0000',
                     message: """\
                     *[${currentBuild.currentResult}] <${env.RUN_DISPLAY_URL}|Build #${env.BUILD_NUMBER}>*
                     ${getChangeType()}: <${getChangeUrl()}|${getChangeName()}>
                     Commit: ${getCommitMessage()} (<${getCommitUrl()}|${getCommitSha().substring(0, 7)}>)
                     Author: <@${getCommitterSlackUserId()}>${isSuccess ? '' : ' (also looping in @channel)'}
                     """.stripIndent()
                 )
            }
        }
    }
}

String getCommitterSlackUserId() {
    if (env.CHANGE_ID)
        branchName = pullRequest.base
    else
        branchName = env.BRANCH_NAME

    endOfBranchPrefix = branchName.indexOf('/')
    if (endOfBranchPrefix == -1)
        return "Unknown (Invalid branch name: ${branchName})"
    committerName = branchName.substring(0, endOfBranchPrefix).toLowerCase()

    gitHubToSlack = [
        "cajan93": "U014UBW46AU",
        "d-justen": "U0144J0QCPM",
        "engelfa": "U014UBW46AU",
        "jansiebert": "U0142D6U51T",
        "jkhlr": "U0149F0BZPW",
        "maltenbergert": "U014GG68EDP",
        "tobodner": "U014FR9CNRF"
    ]
    return gitHubToSlack.containsKey(committerName) ? gitHubToSlack[committerName] : "Unknown (Invalid branch prefix: ${committerName})"
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
    return env.GIT_URL.substring(0, env.GIT_URL.lastIndexOf('.'))
}

String getShellOutput(command) {
    return sh(script: '#!/bin/sh -e\n' + command, returnStdout: true).trim()
}
