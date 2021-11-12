# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a
Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## v4.0.2 - 2021-11-11

### Change

* [PIPELINE-408](https://globalfishingwatch.atlassian.net/browse/PIPELINE-408): Changes
  Normalization date range boundaries.
  Adds gfw-pipeline dependency and reduce the Dockerfile.
  Adds the cloudbuild and fixes the docker-compose.

## v4.0.1 - 2020-08-04

### Removed

* [GlobalFishingWatch/gfw-eng-tasks#143](https://github.com/GlobalFishingWatch/gfw-eng-tasks/issues/143): Removes
    invalid arguments passed to BigQueryCheckOperator.

## v4.0.0 - 2020-07-28
* **BREAKING CHANGE, only compatible with [pipe-vms-generic](https://github.com/GlobalFishingWatch/pipe-vms-generic).**

### Removed

* [GlobalFishingWatch/gfw-eng-tasks#56](https://github.com/GlobalFishingWatch/gfw-eng-tasks/issues/56): Removes
  segment, measures, encounters, anchorages, features and events.

### Changed

* [GlobalFishingWatch/gfw-eng-tasks#56](https://github.com/GlobalFishingWatch/gfw-eng-tasks/issues/56): Changes
  increments Google SDK to `302.0.0`.

## v3.0.3 - 2020-06-11

### Changed

* [GlobalFishingWatch/gfw-eng-tasks#111](https://github.com/GlobalFishingWatch/gfw-eng-tasks/issues/111): Changes
  Pin to `pipe-tools:v3.1.2`.

## v3.0.2 - 2020-05-08

### Changed

* [GlobalFishingWatch/gfw-eng-tasks#84](https://github.com/GlobalFishingWatch/gfw-eng-tasks/issues/84): Changes
  Merged with `master` branch.
  This removes the date_branch and uses raw_all_view as a date_brancher.

## v3.0.1 - 2020-04-25

### Changed

* [GlobalFishingWatch/gfw-eng-tasks#37](https://github.com/GlobalFishingWatch/gfw-eng-tasks/issues/37): Changes
  The table_partition_check parameters `mode`, `poke_interval`, `timeout`,
    `retry_exponential_backoff` by `reties`, `execution_timeout`,
    `retry_dealy` and `max_retry_delay`.

## v3.0.0 - 2020-04-07

### Added

* [GlobalFishingWatch/gfw-eng-tasks#37](https://github.com/GlobalFishingWatch/gfw-eng-tasks/issues/37): Adds
  new distributed events in the Airflow pipeline.

## v2.0.0 - 2020-03-09

### Added

* [GlobalFishingWatch/gfw-eng-tasks#25](https://github.com/GlobalFishingWatch/gfw-eng-tasks/issues/25): Adds
  to support airflow `1.10.5` and ``pipe-tools:v3.1.0``
  Supports `apache-beam:2.16.0`.
  Supports python 3.

## 0.2.0

### Added

* [GlobalFishingWatch/GFW-Tasks#1136](https://github.com/GlobalFishingWatch/GFW-Tasks/issues/1136): Adds
  Move back to having only one flow and used a view that merged NAF and Themis Data

## 0.1.1 - 2019-07-03

### Added

* [GlobalFishingWatch/GFW-Tasks#1085](https://github.com/GlobalFishingWatch/GFW-Tasks/issues/1085): Adds
  Includes the naf_daily processing in the pipe-vms-chile

## 0.1.0

### Added

* Implementation of pipe-vms-chile

