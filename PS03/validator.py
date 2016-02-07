#!/usr/bin/env python
#
# validate and build a student submission
# Must run in Python 2.6 because of Cloudera Quickstart VM

import sys,zipfile

PS="PS03"

required_files = """answers.txt wordcount_top10.py wordcount_top10.txt guanly502_gutenberg_ls.txt guanly502_gutenberg_ls.txt guanly502_gutenberg_top10.txt join1.py join1.txt join2.py join2.txt join3.py join3.txt first50.py first50.txt first50join1.py first50join1.txt sortedjoinbycountry.py sortedjoinbycountry.txt wikipedia_stats.py wikipedia_stats.txt wikipedia_stats.pdf"""

required = required_files.split(" ")

optional = []

required_files = 0
optional_files = 0
errors = 0
from subprocess import Popen,PIPE,call

def build_zip(fname):
    print("Building {0}".format(fname))
    z = zipfile.ZipFile(fname,"w",zipfile.ZIP_DEFLATED)
    for fn in required_files+optional_files:
        if os.path.exist(fn):
            print("Adding {0}...".format(fn))
            z.write(fname)
        else:
            print("Not found {0}...".format(fn))
    print("Done!")
    call(['ls','-l',fname])
    exit(0)


def validate(fname):
    fname = sys.argv[1] if len(sys.argv)>1 else "ps02.zip"
    print("Validating {0}...\n".format(fname))
    z = zipfile.ZipFile(fname)
    for f in z.filelist:
        fname = f.orig_filename
        if fname in required:
            print("Required file {0} present.".format(fname))
            required.remove(fname)
            continue
        if fname in optional:
            print("Optional file: {0} present.".format(fname))
            optional.remove(fname)
            continue
        print("Unwanted file: {0}".format(fname))
        errors += 1

    print("")
    if required:
        print("MISSING REQUIRED FILES: "+" ".join(required))
        errors += len(required)
    if optional:
        print("MISSING OPTIONAL FILES: "+" ".join(optional))
    if errors:
        print("TOTAL ERRORS: {0}".format(errors))
    exit(errors)


if __name__=="__main__":

    fname = PS+".zip"           # default file name
    if len(sys.argv)>1:
        if sys.argv[1]=="--zip":
            build_zip(fname)
        fname = sys.argv[1]
    validate(fname)

