from file_test_utils import *

class ramDisk:
  def __init__(self, path):
    self.path = path

    if not is_directory(self.path):
      os.mkdir(self.path)

    mount_cmd = "mount -t ramfs -o size=2048m ramfs {}".format(self.path)
    subprocess.call(mount_cmd.split())


  def __del__(self):
    umount_cmd = "umount {}".format(self.path)
    subprocess.call(umount_cmd.split())

    if is_directory(self.path):
      shutil.rmtree(self.path)
      os.rmdir(self.path)

    self.path = ''