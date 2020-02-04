import argparse
import os
import shutil
import subprocess as sb


# this is copied and adapted from tripadvisor bidding production script zip_and_ship_afentico_lib.py
# https://gitlab.booking.com/ShopPPC/tripadvisor-bidding-production/blob/master/oozie/zip_afentiko.py
# we need to zip in the right way without additional
# directories in the recursive names
# unfortunately zip -r -j is not viable as
# junking and recursive conflict
# however, we can use python's ability to change directory
# before running subprocesses
# refer to https://stackoverflow.com/questions/21406887/subprocess-changing-directory
def zip_and_ship_module(directory
                        ,copy_from = "/exp_lib/python/"
                        ,copy_to = "/exp_lib"
                        ,zipped_dir = "exp_lib"
                        ,delete_unzipped=True):    
    """Zips and ships any module lib to expected directory
       It also leave in place the unzipped lib unless
       delete unzipped is True

       Args:
           directory : string, the root directory 
           copy_from : string, from which subdirectory to copy the modules in order to generate the scripts
           copy_to   : string, to which subdirectory to create the zipped
           delete_unzipped : boolean, whether to delete the unzipped file or not.
    """

    # copy exp lib to directory
    wd = os.getcwd()
    # generate excluded files for the last step of delete_unzipped
    if delete_unzipped:
        for root, dirs, files in os.walk(wd):
            excluded_files = files
    excluded_files.append(zipped_dir + '.zip')
    
    # ignore .pyc & emacs temp files *~
    shutil.copytree(directory + copy_from, wd + copy_to,
                    ignore = shutil.ignore_patterns("*.pyc","*~"))
    os.chdir(wd)
    # remove *.pyc
    sb.check_output(["zip", "-x", "*.pyc", "-r",  zipped_dir + '.zip', zipped_dir])

    # delete unzipped files if needed
    # using os.walk to walk the directory tree
    # https://unix.stackexchange.com/questions/186163/script-to-delete-files-that-dont-match-certain-filenames

    if delete_unzipped:
        for root, dirs, files in os.walk(wd):
            for f in files:
                if f not in excluded_files:
                    os.remove(f)
            for d in dirs:
                shutil.rmtree(d)

    os.chdir(wd)


