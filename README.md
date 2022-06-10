# Skyrise
Skyrise is a serverless query processor developed by the Enterprise Platform and Integration Concepts Group at HPI. The target workload of Skyrise is interactive in-situ analytics on cold data in cloud storage.

## Key Features

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

## Team
Maintainers: [Thomas Bodner](https://hpi.de/plattner/people/phd-students/thomas-bodner.html), [David Justen]([https://github.com/d-justen](https://hpi.de/plattner/people/phd-students/david-justen.html))

Contributors: [Lars Jonas Bollmeier](https://github.com/BollmeierHPI), [Tobias Maltenberger](https://github.com/maltenbergert), [Julian Menzler](https://github.com/julianmenzler), [Timon Millich](https://github.com/tmillich), [Theo Radig](https://github.com/TheoRadig), [Niklas Riekenbrauck](https://github.com/nikriek), [Daniel Ritter](https://github.com/dritter-sap), [Benedikt Schenkel](https://github.com/Scretch9), [Pascal Schulze](https://github.com/pscls), [Jan Siebert](https://github.com/JanSiebert), [Ben Wegener](https://github.com/bweengener)

Alumni: [Fabian Engel](https://github.com/engelfa), [Jakob Köhler](https://github.com/jkhlr), [Jan Mensch](https://github.com/CAJan93), [Tobias Pietz](https://github.com/Tobias314), [Yannik Schröder](https://github.com/Yanikovic)

## Contributing
If you are a current student at HPI,
- See the New Student Guide.
- Consider enrolling in one of our [database courses](https://hpi.de/plattner/teaching/overview.html).
- Feel free reaching out to us if you are interested in a thesis or student job.
