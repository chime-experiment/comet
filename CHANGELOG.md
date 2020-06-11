# Changelog

All notable changes to this project will be documented in this file. See [standard-version](https://github.com/conventional-changelog/standard-version) for commit guidelines.



### [20.6.0](https://github.com/chime-experiment/comet/compare/2020.04.0...20.6.0) (2020-06-11)


### Bug Fixes

* **archiver:** also catch peewee exceptions if they are renamed again ([01cc999](https://github.com/chime-experiment/comet/commit/01cc9996fa49979f109a91ee84a1f833ff6efef7))


### Features

* **manager:** add option register_state(register_datasets) ([9f87039](https://github.com/chime-experiment/comet/commit/9f87039bf496742c05ee7c8e37ea061f409fecb2))
* **manager:** register_* take and return the objects ([0ac148a](https://github.com/chime-experiment/comet/commit/0ac148ad8a3e1c8faa0deee7ec4619fc9b8a3b67))


### BREAKING CHANGES

* **manager:** Manager.start_state and config_state are State objects now, not IDs
* **manager:** - manager.register_state does not return a state ID anymore, but a State
object.
- manager.register_dataset returns a Dataset object instead of an ID now.




### [2020.04.0](https://github.com/chime-experiment/comet/compare/2019.12.1...2020.04.0) (2020-04-10)


### Bug Fixes

* **broker:** use asyncio.gather ([83592ab](https://github.com/chime-experiment/comet/commit/83592ab))
* **manager:** raise BrokerError for bad HTTP status ([fb563a6](https://github.com/chime-experiment/comet/commit/fb563a6))


### Features

* **archive_everything:** trigger (re-)archiving of all data ([19b8222](https://github.com/chime-experiment/comet/commit/19b8222))
* **broker:** add --timeout (default 40) ([1b77468](https://github.com/chime-experiment/comet/commit/1b77468))
* **manager:** get_dataset requests updates if needed ([0039c08](https://github.com/chime-experiment/comet/commit/0039c08))
* **manager:** get_state asks broker if unknown ([e561b5a](https://github.com/chime-experiment/comet/commit/e561b5a))

### [2019.12.1](https://github.com/chime-experiment/comet/compare/2019.12.0...2019.12.1) (2019-12-19)


### Bug Fixes

* **broker:** don't save invalid datasets ([d507014](https://github.com/chime-experiment/comet/commit/d507014))


### Features

* **broker:** remove debug log from update-datasets ([46e4dc6](https://github.com/chime-experiment/comet/commit/46e4dc6))

## 2019.12.0 (2019-12-13)


### Bug Fixes

* **__init__:** only import Broker for python>2 ([bc6e232](https://github.com/chime-experiment/comet/commit/bc6e232))
* **broker:** disable sanic debug mode ([ecb4d33](https://github.com/chime-experiment/comet/commit/ecb4d33))
* **broker:** don't wait for root ds of invalid ds ([14cb078](https://github.com/chime-experiment/comet/commit/14cb078))
* **broker:** ignore cancellation in endpoints after point of no return ([7e98366](https://github.com/chime-experiment/comet/commit/7e98366))
* **broker:** remove unsued /register-external-state ([bb77294](https://github.com/chime-experiment/comet/commit/bb77294))
* **broker:** set requested state as late as possible ([936f7d9](https://github.com/chime-experiment/comet/commit/936f7d9))
* **broker:** set sanic timeouts to 120s ([88183a1](https://github.com/chime-experiment/comet/commit/88183a1))
* **broker:** sort out task gathering ([6156769](https://github.com/chime-experiment/comet/commit/6156769))
* **broker:** use correct TimeoutError ([e7eb4ec](https://github.com/chime-experiment/comet/commit/e7eb4ec))
* **init:** don't import classes here ([e5ccf95](https://github.com/chime-experiment/comet/commit/e5ccf95))
* **manager:** change 'types' to 'type' ([4a14d39](https://github.com/chime-experiment/comet/commit/4a14d39))
* **broker:** move type from request into the state ([e90c531](https://github.com/chime-experiment/comet/commit/e90c531))
* **manager:** change to use MurmurHash3 for hashing ([325154c](https://github.com/chime-experiment/comet/commit/325154c))
* **manager:** change type of field hash to str ([3ec6578](https://github.com/chime-experiment/comet/commit/3ec6578))
* **manager:** ConnectionError handling ([e90febe](https://github.com/chime-experiment/comet/commit/e90febe))
* **manager:** make hash function python2.7 compatible ([4832a2f](https://github.com/chime-experiment/comet/commit/4832a2f))

### Features

* **archiver:** take input from redis ([6eeac59](https://github.com/chime-experiment/comet/commit/6eeac59))
* **broker:** add port option ([b14b212](https://github.com/chime-experiment/comet/commit/b14b212)), closes [#40](https://github.com/chime-experiment/comet/issues/40)
* **broker:** add simple /status ([909af47](https://github.com/chime-experiment/comet/commit/909af47)), closes [#36](https://github.com/chime-experiment/comet/issues/36)
* **broker:** add timestamp to requested_states ([e0d477f](https://github.com/chime-experiment/comet/commit/e0d477f))
* **broker:** be more verbose about hash collision error ([46454ca](https://github.com/chime-experiment/comet/commit/46454ca))
* **broker:** use redis for shared state ([3b2e36b](https://github.com/chime-experiment/comet/commit/3b2e36b))
* **broker:** use semaphore for worker startup ([1aa3fd6](https://github.com/chime-experiment/comet/commit/1aa3fd6))
* **thread_id:** add logging adapter for thread ids ([41a1a6e](https://github.com/chime-experiment/comet/commit/41a1a6e))

# Changelog



## Version 2019.05 (2019-05-28)

### New Features
- [manager] config is a standalone state
- [manager] add base CometError
- [manager] use file name if caller is not a module
- [manager] add register_start()
- [manager] add public functions
- [broker] add opt-out for dump
- [broker] add output: which dump files are read
- [dumper] dump in locked timestamped files


### Bug Fixes
- [script] use python 3.7
- [__init__] only import Dumper for python>2
- [manager] remove all functionality but register config for now
- [broker] use sanic instead of flask
- [broker] don't reply before dump is finished
- [broker] use DEFAULT_PORT were needed
- [broker] use UTC for all timestamps
- [broker]: only dump hash of re-registered states
- [broker]: sort files by date on recover
- [dumper] remove debug print
- allow state and type to be None for re-register
