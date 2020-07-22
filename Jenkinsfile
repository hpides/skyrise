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
        failure {
            slackSend(
                color: '#FF0000',
                message: "FAILURE: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})"
            )
        }

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
    }
}
