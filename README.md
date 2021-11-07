[![Build Status](https://skyrise-ci.epic-hpi.de/buildStatus/icon?job=skyrise/master)](https://skyrise-ci.epic-hpi.de/blue/organizations/jenkins/skyrise/activity)

# Skyrise

## Building & Tooling

### Quickstart
The Skyrise project is built and tested in a containerized **Docker** environment or on **Ubuntu**. For either setup, run:
```
git clone --recursive git@github.com:hpi-epic/skyrise.git
cd skyrise
```
In a Docker setup, continue with:
```
./script/docker/build_project.sh
```
In a Ubuntu setup, proceed as follows:
```
./script/install_toolchain.sh
mkdir build && cd build
cmake .. -GNinja
ninja all
```

### Test
Skyrise builds on the [GoogleTest](https://github.com/google/googletest) framework. Once built, the target `skyriseTest` can be executed from within a Docker container using the current `skyrise:amazonlinux2` image.

The `skyriseTest` target bundles both

* offline tests and
* online tests, which involve Amazon Web Services. 

To run all tests, call the following convenience script/command from the project's root directory:

```bash
script/docker/build_project.sh -m skyriseTest && script/docker/run_tests.sh
```

Note, however, that the execution of online tests requires AWS credentials. Because Skyrise uses GoogleTest, the flag `--gtest_filter=".."` can be used to run a subset of tests (cf. [googletest/advanced.md](https://github.com/google/googletest/blob/master/docs/advanced.md#running-a-subset-of-the-tests)).

Since online tests are prefixed with `Aws`, they can be excluded from execution as follows:

```bash
script/docker/run_tests.sh --gtest_filter=-Aws*
```

Similarly, offline tests can be excluded from execution as well: 

```bash
script/docker/run_tests.sh --gtest_filter=Aws*
```
