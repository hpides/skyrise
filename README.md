[![Build Status](https://skyrise-ci.epic-hpi.de/buildStatus/icon?job=skyrise/master)](https://skyrise-ci.epic-hpi.de/blue/organizations/jenkins/skyrise/activity)

# Skyrise

## Quickstart
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

## Maintainers

- [Thomas Bodner](https://hpi.de/plattner/people/phd-students/thomas-bodner.html)

## Contributors

- [Fabian Engel](https://github.com/engelfa)
- [David Justen](https://github.com/d-justen)
- [Jakob Köhler](https://github.com/jkhlr)
- [Tobias Maltenberger](https://github.com/maltenbergert)
- [Jan Mensch](https://github.com/CAJan93)
- [Julian Menzler](https://github.com/julianmenzler)
- [Pascal Schulze](https://github.com/orgs/hpi-epic/people/pscls)
- [Jan Siebert](https://github.com/JanSiebert)
