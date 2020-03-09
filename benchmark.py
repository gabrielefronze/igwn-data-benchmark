#! /usr/bin/env python3

import sys
import subprocess
from subprocess import Popen, PIPE
import shutil
import os
import math
import time
import random
from random import shuffle

from fastlog.python.fastlog import *
from file_test_utils import *
from ramdisk import ramDisk

set_log_level(DEBUG)

ramdiskPath = "/mnt/igwn-benchmark-ramdisk"
targetDirectory = "/tmp/igwn-benchmark"
sizePrefixes = ['','K','M','G','T','E','P']


def printBandwidth(measuredBytesPerSecond, rounding=2):
  log = math.log(measuredBytesPerSecond, 2**10)
  roundlog = math.floor(log)
  prefix = sizePrefixes[roundlog]
  bandwidth = measuredBytesPerSecond/math.pow(2**10,roundlog)
  fastlog(WARNING, "Measured bandwidth: {} {}B/s".format(round(bandwidth, rounding), prefix))


def readBenchmark(filesList, loops=1, blocksize=512, pattern='random'):
  bandwidthMeasurements = []
  for file in filesList:
    if pattern=='random':
      fastlog(INFO, "Performing random read benchmark on file {}.".format(file))
      blockscount = math.floor(os.path.getsize(file)/blocksize)
      offsets = list(range(0, blockscount * blocksize, blocksize))
      shuffle(offsets)
    elif pattern=='sequential':
      fastlog(INFO, "Performing sequential read benchmark on file {}.".format(file))
      offsets = [0]
      blocksize = os.path.getsize(file)
    else:
      fastlog(ERROR, "Unsupported read benchmark pattern '{}'. Aborting!".format(pattern))
      return

    partialMeasurements = []

    for loop in range(loops):
      fastlog(DEBUG, "Starting loop {}".format(loop))
      readfile = os.open(file, os.O_RDONLY | os.O_DSYNC, 0o777)
      for i, offset in enumerate(offsets, 1):
        if i%100000 == 0:
          fastlog(DEBUG, "Offset {}/{}".format(i,len(offsets)))
        start = time.time()
        os.lseek(readfile, offset, os.SEEK_SET)
        buff = os.read(readfile, blocksize)
        elapsed = time.time() - start

        if not buff:
          break

        partialMeasurements.append(blocksize/elapsed)
      os.close(readfile)

      bandwidthMeasurements.append(sum(partialMeasurements)/len(partialMeasurements))
      printBandwidth(bandwidthMeasurements[-1])

      partialMeasurements = []


  return bandwidthMeasurements


def IOPSBenchmark(filesList, loops=1, blocksize=512, pattern='random'):
  sectorsize = 4096

  IOPSMeasurements = []

  if pattern=='random':
    fastlog(INFO, "Performing random IOPS measurement")
  elif pattern=='sequential':
    fastlog(INFO, "Performing sequential IOPS measurement")
  else:
    fastlog(ERROR, "Unsupported IOPS measurement pattern '{}'. Aborting!".format(pattern))
    return

  for loop in range(loops):
    fastlog(DEBUG, "Starting loop {}".format(loop))
    for file in filesList:
      fh = os.open(file, os.O_RDONLY | os.O_DSYNC, 0o777)
      blockscount = math.floor(os.path.getsize(file)/blocksize)

      if pattern=='random':
        offsets = list(range(0, blockscount * blocksize, blocksize))
        shuffle(offsets)
      elif pattern=='sequential':
        offsets = [0] * blockscount

      count = 0
      start = time.time()
      for i, offset in enumerate(offsets, 1):
        if i%100000 == 0:
          fastlog(DEBUG, "Offset {}/{}".format(i,len(offsets)))
        os.lseek(fh, offset, os.SEEK_SET)
        blockdata = os.read(fh, blocksize)
        count += 1
              
      elapsed = time.time() - start

      os.close(fh)

      IOPSMeasurements.append(round(count/elapsed))
      fastlog(WARNING,"Measured IOPS: {}".format(IOPSMeasurements[-1]))

  return IOPSMeasurements


def latencyBenchmark(filesList, loops=1):
  for loop in range(loops):
    for file in filesList:
      dd_cmd = "dd if={} of={}/{}".format(file, targetDirectory, file)
      dd_proc = subprocess.Popen(dd_cmd.split(), stdout=PIPE, stderr=subprocess.STDOUT)
      out = dd_proc.communicate()[0].decode('utf-8')
      fastlog(DEBUG, out)


def benchmark(useRamdisk = False, blocksize = 1024, loops = 1, file = None):
  if useRamdisk:
    fastlog(INFO, "Creating ramdisk... ")
    
    targetRamdisk = ramDisk(ramdiskPath)

    global targetDirectory
    targetDirectory = targetRamdisk.path
    
    fastlog(INFO, "Ramdisk created!")
  elif not is_directory(targetDirectory):
    os.mkdir(targetDirectory)

  if file is not None:
    testfiles = [file]
  else:
    testfiles = ["test_file"]

  readBenchmark(testfiles, pattern='random', blocksize=blocksize, loops=loops)
  readBenchmark(testfiles, pattern='sequential', blocksize=blocksize, loops=loops)
  IOPSBenchmark(testfiles, pattern='random', blocksize=blocksize, loops=loops)
  IOPSBenchmark(testfiles, pattern='sequential', blocksize=blocksize, loops=loops)

  if useRamdisk:
    fastlog(INFO, "Unmounting ramdisk... ")
    del targetRamdisk
    fastlog(INFO, "Ramdisk destroyed!")
  elif is_directory(targetDirectory):
    shutil.rmtree(targetDirectory)

import argparse

if __name__ == "__main__":
  parser = argparse.ArgumentParser(description="Measure file access and read performances.")
  parser.add_argument("-bs", "--blocksize", type=int, help='Block size to be used for the tests.')
  parser.add_argument("-l", "--loops", type=int, help='Number of tests to perform for each measure.')
  parser.add_argument("-f", "--file", type=str, help='Input file for the tests.')
  args = parser.parse_args()

  if args.blocksize is not None:
    blocksize = args.blocksize
  else:
    blocksize = 1024

  if args.loops is not None:
    loops = args.loops
  else:
    loops = 1
  fastlog(UI, "Performing benchmark with blocksize {} and {} tests of each kind.".format(blocksize, loops))
  benchmark(blocksize=blocksize, loops=loops)
  fastlog(UI, "Done! Bye bye")