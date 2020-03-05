import os
import os.path

def is_file(fpath):
    return os.path.isfile(fpath)

def is_directory(fpath):
    return os.path.isdir(fpath)

def is_exe(fpath):
    return is_file(fpath) and os.access(fpath, os.X_OK)

def is_abs_path(fpath):
    return fpath == os.path.abspath(fpath)