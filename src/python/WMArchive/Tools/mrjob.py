#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : mrjob.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: Basic tool to run MapReduce job via pydoop
"""

# futures
from __future__ import print_function, division

# system modules
import os
import sys
import stat
import time
import inspect
import argparse
from tempfile import NamedTemporaryFile

# pydoop modules
try:
    import pydoop.hdfs as hdfs
    PYDOOP = True
except:
    PYDOOP = False

# WMArchive modules
from WMArchive.MapReduce.Skeleton import Reader

def skeleton_file(verbose=None):
    "Return location of skeleton file"
    sname = inspect.getfile(Reader)
    if  sname.endswith('pyc'):
        sname = sname[:-1] # use .py instead of .pyc
    if  verbose:
        print("Skeleton %s" % sname)
    return sname

def usage():
    "Generate usage message"
    sfile = skeleton_file()
    efile = sfile.replace('Skeleton', 'mruser')
    msg = """
Tool to generate and/or execute MapReduce (MR) script. The code is generated
from MR skeleton provided by WMArchive and user based MR file. The later must
contain two functions: mapper(ctx) and reducer(ctx) for given context. Their
simplest implementation can be found here
%s
Based on this code please create your own mapper/reducer functions and use
this tool to generate final MR script.""" % efile
    return msg

class OptionParser():
    def __init__(self):
        "User based option parser"
        self.parser = argparse.ArgumentParser(prog='mrjob', description=usage())
        self.parser.add_argument("--hdir", action="store",
            dest="hdir", default="/test", help="HDFS input data directory")
        self.parser.add_argument("--odir", action="store",
            dest="odir", default="/mrout", help="HDFS output directory for MR jobs")
        self.parser.add_argument("--schema", action="store",
            dest="schema", default="/schema.avsc", help="Data schema file on HDFS")
        self.parser.add_argument("--mrpy", action="store",
            dest="mrpy", default="mr.py", help="MapReduce python script")
        self.parser.add_argument("--pydoop", action="store",
            dest="pydoop", default="pydoop.tgz", help="pydoop archive file, e.g. /path/pydoop.tgz")
        self.parser.add_argument("--avro", action="store",
            dest="avro", default="avro.tgz", help="avro archive file, e.g. /path/avro.tgz")
        self.parser.add_argument("--execute", action="store_true",
            dest="execute", default=False, help="Execute generate mr job script")
        self.parser.add_argument("--verbose", action="store_true",
            dest="verbose", default=False, help="Verbose output")

def run(cmd):
    "Run given command in subprocess"
    print(cmd)
    proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    proc.wait()
    out = proc.stdout.read()
    err = proc.stderr.read()
    proc.stdout.close()
    proc.stderr.close()
    if  out:
        print("ERROR:", out)
        sys.exit(1)
    return parse(err)

def hdfs_dir(hdir):
    "Return HDFS URI for given directory"
    hdfs_prefix = 'hdfs://p01001532965510.cern.ch:9000'
    if  hdir.startswith('hdfs://'):
        return hdir
    if  hdir.startswith('/'):
        return '%s%s' % (hdfs_prefix, hdir)
    return '%s/%s' % (hdfs_prefix, hdir)

def create_mrpy(usermr, verbose=None):
    "Create MR python script from skeleton and user provided code"
    sname = skeleton_file(verbose)
    code = open(sname).read() + '\n' + open(usermr).read()
    return code

def mrjob(hdir, odir, schema, mrpy, execute, arch_pydoop, arch_avro, verbose):
    "Generates and executes MR job script"
    hdir = hdfs_dir(hdir)
    odir = hdfs_dir(odir)
    schema = hdfs_dir(schema)

    if  PYDOOP:
        for name in [hdir, odir,]:
            if  verbose:
                print("Checking %s" % name)
            if  not hdfs.path.isdir(name):
                print("ERROR: %s does not exists" % name)
                sys.exit(1)
        if  verbose:
            print("Checking %s" % schema)
        if  not hdfs.path.isfile(schema):
            print("ERROR: %s does not exists" % name)
            sys.exit(1)
    else:
        if  verbose:
            print("WARNING: hdfs module is not present on this system, will use input as is without checking")
    for name in [mrpy, arch_pydoop, arch_avro]:
        if  verbose:
            print("Checking %s" % name)
        if  not os.path.isfile(name):
            print("ERROR: %s does not exists" % name)
            sys.exit(1)
    module = mrpy.split('/')[-1].split('.')[0]
    code = create_mrpy(mrpy, verbose)
    user = os.getenv('USER')
    tstamp = int(time.time())

    cmd = """#!/bin/bash
input={input}
output={output}
ifile=/tmp/mr_{user}_{tstamp}.py
hadoop fs -rm -r $output
cat << EOF > $ifile
{code}
EOF
ifile={ifile}
module={module}
arch_pydoop={pydoop}
arch_avro={avro}
echo "Input URI : $input"
echo "Output URI: $output"
echo "MR script : $file"
echo "Pydoop archive: $arch_pydoop"
echo "Avro archive  : $arch_avro"
pydoop submit \
    --upload-archive-to-cache $arch_pydoop \
    --upload-archive-to-cache $arch_avro \
    --do-not-use-java-record-reader \
    --log-level DEBUG \
    --num-reducers 1 \
    --upload-file-to-cache $ifile \
    --mrv2 $module $input $output
    """.format(input=hdfs_dir(hdir), output=hdfs_dir(odir), user=user, tstamp=tstamp,
            code=code, ifile=mrpy, module=module, pydoop=arch_pydoop, avro=arch_avro)
    fobj = NamedTemporaryFile(delete=False)
    fobj.write(cmd)
    fobj.close()
    fstat = os.stat(fobj.name)
    os.chmod(fobj.name, fstat.st_mode | stat.S_IEXEC)
    if  execute:
	run(fobj.name)
    else:
        if  verbose:
            print("------- Generated script --------")
        print(open(fobj.name, 'r').read())
        if  verbose:
            print("---------------------------------")
    os.unlink(fobj.name)

def main():
    "Main function"
    optmgr  = OptionParser()
    opts = optmgr.parser.parse_args()
    mrjob(opts.hdir, opts.odir, opts.schema, opts.mrpy,
            opts.execute, opts.pydoop, opts.avro, opts.verbose)

if __name__ == '__main__':
    main()
