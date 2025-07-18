#Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## 0.7.4
- raid1: Fix degraded discard

## 0.7.3
- raid1: Bitmap Load/Init

## 0.7.2
- General fixes

## 0.7.1
- raid1: Writes in degraded mode

## 0.7.0
- raid1: Extract BITMAP into its own class
- raid1: Reads in degraded mode

## 0.6.x
- raid1: Do not re-write unchanged pages
- raid1: Round-Robin reading
- homeblk_disk: Disable by default
- raid1: Calculate reserved area based on limits
- ublkpp_tgt: Clear async_event before calling process_result
- raid1: Bitmap words should be encoded as NETWORK byte order
- raid1: Records dirty chunks to the BITMAP pages

## 0.5.x
- homeblk_disk : introduced
- raid1: more intelligent retry handling
- ublkpp_tgt : fix narrowing conversion

## 0.4.x
- iscsi_disk : introduced

## 0.3.x
- ublkpp_tgt : improvments

## 0.2.x
- ublkpp_tgt : API changes

## 0.1.x
- fs_disk : introduced
- raid0/1 : introduced
