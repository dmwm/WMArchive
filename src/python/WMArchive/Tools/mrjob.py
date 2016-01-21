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
import subprocess
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
Tool to generate MapReduce (MR) scripts and launch jobs. The code is
generated from a skeleton provided by WMArchive and a user based MR file. The
latter must contain definitions for two functions: mapper(ctx) and
reducer(ctx) for given context. A trivial example can be found in:
%s
Based on this code please create your own mapper/reducer functions and use
this tool to generate the final MR script.""" % efile

    return msg

class OptionParser(object):
    "User based option parser"
    def __init__(self):
        try:
            remdir = os.path.join('/user', os.getenv('USER'))
        except Exception as exp:
            print("Failed to find username", exp)
            remdir = '/'
        self.parser = argparse.ArgumentParser(prog='mrjob',\
                                              description=usage())
        self.parser.add_argument("--hdir", action="store",\
            dest="hdir", default=os.path.join(remdir, 'test/'),\
            help="hdfs base directory, default=%(default)s")
        self.parser.add_argument("--idir", action="store",\
            dest="idir", default='data/',\
            help=("hdfs input directory for MR jobs, inside hdir, "
                  "default=%(default)s"))
        self.parser.add_argument("--odir", action="store",\
            dest="odir", default='mrout/',\
            help=("hdfs output directory for MR jobs, inside hdir, "
                  "default=%(default)s"))
        self.parser.add_argument("--schema", action="store",\
            dest="schema", default='schema.avsc',\
            help=("Name of data schema file on hdfs, inside hdir, "
                  "default=%(default)s"))
        self.parser.add_argument("--mrpy", action="store",\
            dest="mrpy", default="mr.py", help="MapReduce python script")
        self.parser.add_argument("--pydoop", action="store",\
            dest="pydoop", default="pydoop.tgz",\
            help="pydoop archive file, e.g. /path/pydoop.tgz")
        self.parser.add_argument("--avro", action="store",\
            dest="avro", default="avro.tgz",\
            help="avro archive file, e.g. /path/avro.tgz")
        self.parser.add_argument("--execute", action="store_true",\
            dest="execute", default=False,\
            help="Execute the generated mr job script (i.e. launch the job)")
        self.parser.add_argument("--verbose", action="store_true",\
            dest="verbose", default=False, help="Verbose output")
        self.parser.add_argument("--loglevel", action="store",\
            dest="loglevel", default='INFO',\
            help="pydoop log level, default=%(default)s")
        self.parser.add_argument("--hdfs_prefix", action="store",\
            dest="hdfs_prefix",\
            default='hdfs://p01001532965510.cern.ch:9000',\
            help="hdfs prefix, default=%(default)s")

def run(cmd, verbose=False):
    "Run given command in subprocess"
    if verbose:
        print('Executing', cmd)

    # proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    proc = subprocess.Popen(cmd, shell=True, stdout=sys.stdout, stderr=sys.stderr)
    proc.wait()
    return proc.returncode

def hdfs_dir(hdir, prefix='hdfs://p01001532965510.cern.ch:9000'):
    "Return HDFS URI for given directory"
    if  hdir.startswith('hdfs://'):
        return hdir
    if  hdir.startswith('/'):
        return '%s%s' % (prefix, hdir)
    return '%s/%s' % (prefix, hdir)

def create_mrpy(usermr, verbose=None):
    "Create MR python script from skeleton and user provided code"
    sname = skeleton_file(verbose)
    code = open(sname).read() + '\n' + open(usermr).read()
    return code

def mrjob(options):
    "Generates and executes MR job script"

    user = os.getenv('USER')
    tstamp = int(time.time())
    hdir = hdfs_dir(options.hdir, options.hdfs_prefix)

    if  PYDOOP:
        odir = hdfs.path.join(hdir, options.odir)
        idir = hdfs.path.join(hdir, options.idir)
        schema = hdfs.path.join(hdir, options.schema)
        for name in [hdir, odir, idir,]:
            if  options.verbose:
                print("Checking %s" % name)
            if  not hdfs.path.isdir(name):
                if name in [hdir, idir]:
                    print("ERROR: %s does not exist" % name)
                    sys.exit(1)
                # else:
                #     print(" Creating output directory: %s" % name)
                #     hdfs.mkdir(name)
            elif name == odir:
                # in case odir exists and is not empty, move it somewhere and re-create
                if hdfs.ls(odir):
                    ocache = hdfs.path.normpath(odir)+'_%d'%tstamp
                    if options.verbose:
                        print(" Non-empty output directory exists, saving it in %s"%ocache)
                    hdfs.move(odir, ocache)
                    # hdfs.mkdir(odir)
                # if it's empty, remove it
                else:
                    hdfs.rmr(odir)

        if  options.verbose:
            print("Checking %s" % schema)
        if  not hdfs.path.isfile(schema):
            print("ERROR: %s does not exist" % schema)
            sys.exit(1)
    else:
        idir = '%s%s' % (hdir, 'data')
        odir = '%s%s' % (hdir, 'mrout')
        schema = '%s%s' % (hdir, options.schema)
        if  options.verbose:
            msg = 'pydoop module is not present on this system'
            msg += ', will use input as is without checking'
            print('WARNING:', msg)
    for name in [options.mrpy, options.pydoop, options.avro]:
        if  options.verbose:
            print("Checking %s" % name)
        if  not os.path.isfile(name):
            print("ERROR: %s does not exist" % name)
            sys.exit(1)

#     module = os.path.basename(os.path.splitext(options.mrpy)[0])
    code = create_mrpy(options.mrpy, options.verbose)

    cmd = """#!/bin/bash
input={input}
output={output}
schema={schema}
ifile=/tmp/mr_{user}_{tstamp}.py
cat << EOF > $ifile
{code}
EOF

module=mr_{user}_{tstamp}
arch_pydoop={pydoop}
arch_avro={avro}
echo "Input URI : $input"
echo "Output URI: $output"
echo "Schema: $schema"
echo "MR script : $ifile"
echo "Module name : $module"
echo "Pydoop archive: $arch_pydoop"
echo "Avro archive  : $arch_avro"
echo "-----------------"
echo "Submitting MR job"
pydoop submit \
    --upload-archive-to-cache $arch_pydoop \
    --upload-archive-to-cache $arch_avro \
    -D avro.schema=$schema \
    --do-not-use-java-record-reader \
    --log-level {loglevel} \
    --job-name WMArchive \
    --num-reducers 1 \
    --upload-file-to-cache $ifile \
    --mrv2 $module $input $output
    """.format(input=idir, output=odir, user=user, tstamp=tstamp,
               code=code, schema=schema, loglevel=options.loglevel,
               pydoop=os.path.abspath(options.pydoop),
               avro=os.path.abspath(options.avro))

    fobj = NamedTemporaryFile(delete=False)
    fobj.write(cmd)
    fobj.close()

    fstat = os.stat(fobj.name)
    os.chmod(fobj.name, fstat.st_mode | stat.S_IEXEC)

    if  options.execute:
        run(fobj.name, options.verbose)
    else:
        if  options.verbose:
            print("------- Generated script --------")
        print(open(fobj.name, 'r').read())
        if  options.verbose:
            print("---------------------------------")

    # clean up temporary file
    os.unlink(fobj.name)

def main():
    "Main function"
    optmgr = OptionParser()
    opts = optmgr.parser.parse_args()
    mrjob(opts)

if __name__ == '__main__':
    main()
