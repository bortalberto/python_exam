import glob2
import sys
import pickle
"""
Checks for the OS and select the correct separator
"""
OS = sys.platform
if OS == 'win32':
    sep = '\\'
elif OS == 'linux' or "linux2":
    sep = '/'
else:
    print("ERROR: OS {} non compatible".format(OS))
    sys.exit()


class Run_Configuration:

    def __init__(self, run_path):
        """
        This class contains the electronics configurations
        """
        self.run_path = run_path
        self.conf_dict = self._build_subruns_configurations()

    def _build_subruns_configurations(self):
        """
        Run into the run_folder building a nested dictionary containing all the sub runs configurations
        """
        sub_run_dict = {}
        for conf_file, (subrun_number,) in glob2.iglob(self.run_path + sep + "CONF_log_*.pkl", with_matches=True):
            with open(conf_file, 'rb') as f:
                sub_run_dict["sub_run {}".format(subrun_number)] = pickle.load(f)

        return sub_run_dict


if __name__ == "__main__":
    TL_run_8 = Run_Configuration("/home/alb/corso_python/exam_project/data"+sep+"RUN_8")
