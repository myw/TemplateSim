#!/usr/bin/env python

## Copyright (C) 2011 Mikhail Wolfson <wolfsonm@mit.edu>
##
## This file is part of TemplateSim.
##
## TemplateSim is free software; you can redistribute it and/or modify
## it under the terms of the GNU General Public License as published by
## the Free Software Foundation; either version 2 of the License, or
## (at your option) any later version.
## 
## TemplateSim is distributed in the hope that it will be useful,
## but WITHOUT ANY WARRANTY; without even the implied warranty of
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
## GNU General Public License for more details.
## 
## You should have received a copy of the GNU General Public License
## along with this program; if not, write to the Free Software 
## Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.

#
# make_job.py -- Use a template directory and an options file to construct
#                a specific set of parameters for a simulation.


from __future__ import with_statement

import sys
import os
import re
import shutil
import logging
import csv
import StringIO

from datetime import datetime
from glob import iglob
from subprocess import Popen
from subprocess import PIPE

from ConfigParser import SafeConfigParser
from optparse import OptionParser

logging.basicConfig(level = logging.DEBUG, stream = sys.stderr)

# Multiline manipulation function factory
def make_multiline_manip(prefix, joiner, condition = lambda line: True):
    """Create multi-line manipulators with appropriate prefixes and joiners.

    Returns a closure of a function. Condition parameter defaults to always
    picking lines, but can be replaced with a function that only picks select
    lines"""
    def manip(str):
        """Manipulate a multi-line string.

        Parse a string by lines, comment each out in front with a hash, and
        rejoin."""
        return joiner.join([prefix + line for line in str.split('\n')
            if condition(line)])

    return manip

# Add comments to a multi-line string.
comment = make_multiline_manip(prefix = "# ", joiner = '\n')
single_line = make_multiline_manip(prefix = '', joiner = "; ",
        # Ignore blank lines
        condition = lambda line: re.search(r"^\s*$", line) == None)

def config_to_string(cp):
    """Turn the contents of a ConfigParser into a single string."""

    config_str = StringIO.StringIO()
    cp.write(config_str)
    config_text = config_str.getvalue()
    config_str.close()

    return config_text

def make_link(src, dest):
    """Log making a symlink."""
    logging.info("[symlink] %s -> %s", dest, src)
    os.symlink(src, dest)

def copy_file(src, dest):
    """Log copying a file."""
    logging.info("[copy] %s -> %s", src, dest)
    shutil.copy(src, dest)

def setup_directory(prefix):
    """Create a new directory with the supplied prefix."""

    ## Find the previously largest-numbered
    ##  directory with the same prefix

    # Init
    maxnum = 0
    findnum = re.compile(r"\D*(\d+)") # Number at the end

    # Search through directories with the right prefix
    for dir in iglob(prefix + '*'):

        # Parse out their number
        num = findnum.search(dir)

        if num == None:
            continue
        num = int(num.group(1))

        # Update the maximum
        if num > maxnum:
            maxnum = num

    # The right dirname is the one after the maximum
    dirname = prefix + str(maxnum + 1)

    logging.info("[mkdir] %s", dirname)
    os.makedirs(dirname)
    return dirname

def copy_static_files(config, template, destdir):
    """Just copy over a set of files form the template to the job directory.
    
    Files are unchanged as much as possible, and their permissions and stats are
    kept unchanged."""

    static_files = ["nanzscore.m",        
                    "run_ga_script.m",    
                    "test_subset_model.m"];

    for sf in static_files:
        src = os.path.join(template, sf)
        dest = os.path.join(destdir, os.path.basename(sf))
        copy_file(src, dest)

def copy_datafile(datafile, template, destdir):
    """Copy over a file, and make a general-name link to it."""

    datafile_src = os.path.join(template, datafile)
    datafile_dest = os.path.join(destdir, datafile)

    # the datafile has a name of the form root-tag.ext. Split out the root and the
    # extension, and use those for the destination link
    link_name = os.path.join(destdir,
                             datafile.split('-')[0] +
                             os.path.splitext(datafile)[1])
    copy_file(datafile_src, datafile_dest)
    make_link(datafile, link_name)


def create_options_file(alg_opts, template, destdir):
    """Create a real options file, using the template and parameters.
    
    Case in the option names is preserved."""

    options_file_name = "ga_options.m"

    def format_opts(opt_pairs, spacing = 0):
        opts = ""

        for (key, value) in opt_pairs:
            opts += ' ' * spacing + "'%s', %s, ...\n" % (key, value)

        return opts[:-1] # skip the last \n

    ifn = os.path.join(template, options_file_name)
    ofn = os.path.join(destdir, options_file_name)

    logging.info("[parse] %s -> %s", ifn, ofn)

    with open(ifn) as ifile:
        with open(ofn, 'w') as ofile:
            for line in ifile:
                ofile.write(sub_tags(line, {
                    "<OPTIONS>": format_opts(alg_opts, spacing = 4)
                }))

def sub_tags(line, subst):
    """Replace tags in a line.
    
    Given a line and a dictionary of tags and replacement values, find the first
    tag in the dictionary that the line matches, replace it with its substituted
    value, and return.

    XXX Lines should not have more than one tag. If they do, which tag is replaced
    is not guaranteed to be consistent.
    """

    # Look through all the tags
    for (tag, rep) in subst.iteritems():
        # The first replacement we find, do it and return early
        if line.find(tag) >= 0:
            return line.replace(tag, rep)

    # If we didn't find any tags, just return the original line
    return line

def create_job_file(template, destdir, config, queue):
    """Create a real job file, using the template and parameters."""

    name = os.path.basename(destdir)
    template_job_file_name  = os.path.join(template, "job_file.job")
    new_job_file_name = os.path.join(destdir, name + ".job")

    logging.info("[parse] %s -> %s", template_job_file_name, new_job_file_name)

    with open(template_job_file_name) as ifile:
        with open(new_job_file_name, 'w') as ofile:
            for line in ifile:
                ofile.write(sub_tags(line, {
                    "<NAME>":   name,
                    "<QUEUE>":  queue,
                    "<DATE>":   datetime.today().ctime(),
                    "<CONFIG>": comment(config)
                }))


    shutil.copystat(template_job_file_name,  new_job_file_name)

    return new_job_file_name

def submit_job(job_file):
    """Given a job file, run qsub on it and record its output (the job number)."""

    #             Create a qsub, pipint its stdout   catch output  show stdout
    qsub_output = Popen(("qsub", job_file), stdout=PIPE).communicate()[0]

    logging.info("[submit] %s: %s", job_file, qsub_output)

    # Return only the number part
    return re.search(r"^(\d+)\.", qsub_output).group(1)

def update_db(file, run, config, job_num = None):
    """Update the job database (in csv) with one job's information."""

    data = []
    data.append(run)
    data.append(datetime.today().ctime())
    data.append(single_line(config))

    if job_num != None:
        data.append(job_num)

    with open(file, 'a') as ofile:
        writer = csv.writer(ofile)
        writer.writerow(data)

    logging.info("[update-db] %s", file)

def make_job(cp, opts):
    """Make a job directory from a specific set of options."""

    logging.info("[exec] %s", " ".join(sys.argv))

    config_text = config_to_string(cp)

    logging.info("[config] %s\n%s", config_file, config_text)

    dirname = setup_directory(opts.prefix)
    copy_static_files(config = config_file,
                      template = opts.template_dir,
                      destdir = dirname)

    # Write individual config file to destination directory
    with open(os.path.join(dirname, dirname + ".cfg"), 'w') as ofile:
        cp.write(ofile)

    # The following are the actual parsing and copying core actions of the
    # script: the data file(s), options file, and job file
    copy_datafile(datafile = cp.get("Files", "data"),
                  template = opts.template_dir,
                  destdir = dirname)
    create_options_file(alg_opts = cp.items("Algorithm"),
                        template = opts.template_dir,
                        destdir = dirname)
    job_file = create_job_file(template = opts.template_dir,
                               destdir = dirname,
                               config = config_text,
                               queue = cp.get("Job", "queue"))

    # Finally, submit the job if the user wants it
    if opts.submit:
        job_num = submit_job(job_file)
    else:
        job_num = None

    # And update the jobs db to reflect the job
    update_db(file = opts.database_file,
              run = dirname,
              config = config_text,
              job_num = job_num)

if __name__ == "__main__":
    # Parse command line arguments
    op = OptionParser(usage = "%prog [options] <config_file>",
                      description = "make a GA variable selection job " + \
                      "directory by applying the options in <config_file> " + \
                      "to the files in TEMPLATE_DIR, which defaults to 'template'",
                      add_help_option = True,
                      version = "0.1")
    op.add_option("-t", "--template_dir", default = "template",
                  help = "use TEMPLATE_DIR for the template files")
    op.add_option("-d", "--database_file", default = "jobs.db",
                  help = "use DATABASE_FILE for the jobs database")
    op.add_option("-p", "--prefix", default = "run-",
                  help = "use PREFIX as the job directory prefix")
    op.add_option("-v", "--verbose", action = "store_true",
                  help = "detailed status updates")
    op.add_option("-s", "--submit", action = "store_true",
                  help = "submit the job immedately after creation")
    (opts, args) = op.parse_args()

    if len(args) < 1:
        op.error("must provide config file")

    # Parse config file
    config_file = args[0]
    cp = SafeConfigParser()

    # Have ConfigParser treat its options as case-sensitive
    cp.optionxform = str


    cp.read(config_file)

    if not opts.verbose:
        logging.disable(logging.INFO)


    # Interperet data as a glob, and make a job for every file that
    # matches it
    data_files = iglob(os.path.join(opts.template_dir, cp.get("Files", "data")))
    for data_file in data_files:
        cp.set("Files", "data", os.path.basename(data_file))
        make_job(cp, opts)

