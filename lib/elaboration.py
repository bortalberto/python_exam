from lib import TIGER_data_importer as data_imp
from lib import configurator_importer as conf_imp
import matplotlib.pylab as plt
import os
import sys

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


class Run:
    def __init__(self, run_path, mode = "TL"):
        """
        This class contains the data taken in each run and the chips configurations
        TL stands for TriggerLess mode (in this mode, the chip send a continuous stream of data)
        """
        self.run_path = run_path
        self._directory_check()
        self.configuration = conf_imp.Run_Configuration(self.run_path)
        if mode == "TL":
            self.data = data_imp.Run_Data_TL(self.run_path)
        if mode == "TM":
            self.data = data_imp.Run_Data_TM(self.run_path)

    def _directory_check(self):
        """
        Check the existence of the data directory and that it's not empty
        """

        if os.path.exists(self.run_path) and os.path.isdir(self.run_path):
            if not os.listdir(self.run_path):
                raise Exception("Run data directory is empty")
        else:
            raise Exception("Run data Directory doesn't exists")

    def plot_sub_runs_rates(self):
        """
        Plots the number of hits in evert subrun
        """
        self.data.hit_df[(self.data.hit_df["word_type"] == "hit")].hist(column="sub_run")
        plt.title("Hit per subrun")
        plt.ylabel("Hits")
        plt.xlabel("Sub run")
    def measure_efficency(self,GEMROC):
        """
        Measure the efficency like the number of test pulses detected over the number of test pulses sent
        """
        grouped_data = self.data.hit_df.groupby("GEMROC")
        this_gemroc_data = grouped_data.get_group(GEMROC)  # Selected the data only from the reference GEMROC
        for sub_run in this_gemroc_data["sub_run"].unique():  # Calculate fro each subrun
            number_of_TP_per_frame = self.configuration.conf_dict["sub_run {}".format(sub_run)]["GEMROC 0"]["DAQ"]["number_of_repetitions"] - 512  # Get the number of produced test pulses from the configuration dictionary
            number_of_frameword = (this_gemroc_data[(this_gemroc_data["word_type"] == "frame") & (this_gemroc_data["sub_run"] == sub_run)].count()["frame_count"])
            total_TP = number_of_frameword * number_of_TP_per_frame
            total_hit = this_gemroc_data[(this_gemroc_data["word_type"]== "hit") & (this_gemroc_data["sub_run"]== sub_run)].count()["Channel"]   # Count the number of hits
            print ("Efficency with {} TPs: {}".format(number_of_TP_per_frame, total_hit/total_TP))  # print efficency
if __name__ == "__main__":
    TL_run_10 = Run("/home/alb/corso_python/exam_project/data"+sep+"RUN_10")
    # TL_run_8.plot_sub_runs_rates()
    TL_run_10.measure_efficency(0)