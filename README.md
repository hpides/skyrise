# Skyrise

## Quickstart
The Skyrise project is built and tested in a containerized **Docker** environment or on **Ubuntu 20.10**.
```
git clone --recursive git@github.com:hpi-epic/skyrise.git
cd skyrise
```

```
./script/docker/build_project.sh
```

```
./script/install_toolchain.sh
mkdir cmake-build-debug && cd cmake-build-debug
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
