# Master Coverage Check

This action compares test code coverage of a local branch with the master branch. It is used to reject all PRs that have a lower coverage than the current master branch has. The coverage of the master branch is persisted and maintained in an object in Amazon S3. 

## Building

The file `dist/index.js` contains the logic of this custom action bundled with all dependencies. A GitHub Actions worker will run this file. Whenever the source code in `src/main.ts` is updated, a new artifact needs to be built and commited. Before commiting, run `npm install` to install dependencies and `npm run all` to build.