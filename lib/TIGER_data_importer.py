import os
import glob2
import pandas as pd
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


class Run_Data_TL:
    def __init__(self, run_path):
        """
        This class contains the data taken in each run and the chips configurations
        TL stands for TriggerLess mode (in this mode, the chip send a continous stream of data)
        """
        self.mode = str(self.__class__).split("Run_Data_")[1][0:2]  # This gets the last part of the class name, allowing me to distinguish if it's running in TL or TM mode
        self.run_path = run_path
        if os.path.isfile(self.run_path + sep + "pandas_df_save"):
            self.load_dataframe()  # Since the creation of the data can take some times, the data are saved automatically and loaded at each successive run
        else:
            print("Data-frame doesn't exist, will create a new one ")
            self.hit_df = pd.DataFrame(self._build_subruns_data())
            print("Data-frame created ")
            if self.mode == "TL":
                print("Assigning frameword to hit (may take a while)")
                self.assign_frameword_to_hit()
        self.save_dataframe()

    def _build_subruns_data(self):
        """
        Run into the run_folder building a dictionary containing all the sub runs data

        """
        word_list = []
        for hit_file, (subrun_number, gemroc_number, mode) in glob2.iglob(self.run_path + sep + "SubRUN_*_GEMROC_*_*.dat", with_matches=True):
            # For first, runs a couple of checks on the file
            if mode != self.mode:
                raise Exception("Wrong trigger mode (got {}), maybe you want to use the other mode?".format(mode))
            file_size = os.stat(hit_file).st_size  # Reads the number of Bytes in the file
            if file_size % 8 != 0:
                raise Exception("The file length is not a multiple of 8 Bytes, something bad happen saving the file")
            with open(hit_file, 'rb') as fi:
                word_number = 0
                while True:  # Reads all the file. If TIGER_word_raw is empty, the end of the file is reached, each event is parsed into a dictionary and put in the global list
                    TIGER_word_raw = fi.read(8)
                    if not TIGER_word_raw:
                        break
                    word_number += 1
                    event_word_dict = {
                        "word_number":  int(word_number),
                        "sub_run":      int(subrun_number),
                        "GEMROC":       int(gemroc_number),
                    }  # Generic information for each word
                    event_word_dict.update(self._parser(TIGER_word_raw))  # Add the hit information to the generic word information

                    word_list.append(event_word_dict)
        return word_list

    def _parser(self, data):
        """
        Parses a TIGER word
        To read the data correctly, the Byte order needs to be inverted.
        The 8 bytes can be handled like a list of bytes, and then read by their meaning
        Data structure for hit words
         Byte            0			      	  | 			1				  |			2					  |			3					  |    		4					  |		5						  |		6						  |		7                        |
         Bit    7	6	5	4	3	2	1	0	7	6	5	4	3	2	1	0	7	6	5	4	3	2	1	0	7	6	5	4	3	2	1	0	7	6	5	4	3	2	1	0	7	6	5	4	3	2	1	0	7	6	5	4	3	2	1	0	7	6	5	4	3	2	1	0
         Data  Type   | Empty      |Chip numb | Empty | 			CHANNEL	  |TAC	  |       T coarse			    								  |					E corse				  |	  					T fine			  |	        					E fine   |
        """
        this_word = {}
        reversed_data = (list(reversed(data)))  # Swaps the byte order
        if (reversed_data[0] >> 5) == 1:  # It's a frameword
            this_word = {
                "word_type":    "frame",
                "TIGER":         (reversed_data[0]) & 0x7,
                "frame_count":  (reversed_data[4] << 9 | reversed_data[5] << 1 | reversed_data[6] >> 7) & 0xFFFF,
                "SEU_count":    (reversed_data[6] << 8 | reversed_data[7]) & 0x7FFF
            }

        if (reversed_data[0] >> 5) == 0:  # It's a hit
            this_word = {
                "word_type":    "hit",
                "TIGER":        (reversed_data[0]) & 0x7,
                "Channel":      (reversed_data[1]) & 0x3F,
                "TAC":          (reversed_data[2] >> 6) & 0x3,
                "T_coarse":     (reversed_data[2] << 10 | reversed_data[3] << 2 | reversed_data[4] >> 6) & 0xFFFF,
                "E_coarse":     (reversed_data[4] << 4 | reversed_data[5] >> 4) & 0x3FF,
                "T_fine":       (reversed_data[5] << 6 | reversed_data[6] >> 2) & 0x3FF,
                "E_fine":       (reversed_data[6] << 8 | reversed_data[7]) & 0x3FF

            }
        if (reversed_data[0] >> 5) == 2:  # It's a countword
            # not used up to now
            raise Exception("Count word found, but they are not in use, maybe something went wrong in the data transmission or in the parsing")
        return this_word

    def assign_frameword_to_hit(self):
        """
        This function assign at each hit, the correspondent frameword. The "orphan" hits are deleted (the first few hits in every subrun)
        The data are divided group by hardware and sub_run, the "apply" function apply the functions to every group like it is a separate dataframe
        """
        self.hit_df = self.hit_df.groupby(["sub_run", "GEMROC"], as_index=False).apply(_assign_frameword_group)
        self.hit_df = self.hit_df.groupby(["sub_run", "GEMROC"], as_index=False).apply(_remove_orphan_words)
        self.hit_df = self.hit_df.reset_index()  # Reset the index after the rows drops
        self.hit_df.drop(self.hit_df.columns[0:2], axis=1, inplace=True)  # The grouping and the re-indexing created two unnecessary columns, deleted here

    def save_dataframe(self):
        """
        In order to speed up the operations, the data can be saved and loaded using the pickle extension (already integrated in pandas)
        """
        self.hit_df.to_pickle(self.run_path + sep + "pandas_df_save")

    def load_dataframe(self):
        """
        In order to speed up the operations, the data can be saved and loaded using the pickle extension (already integrated in pandas)
        """
        self.hit_df = pd.read_pickle(self.run_path + sep + "pandas_df_save")


def _remove_orphan_words(data):
    """
    Removes the hits without the reference frameword and the frameword from the beginning and the end of the data
    """
    data = data[data.frame_count.isnull() == False]
    return data[1:-1]


def _assign_frameword_group(data):
    """
    This function exist only to apply the function  "_find_previous_frameword" to each row of the data frame
    Passing the slice of the data before the row instead of the whole data saves some time (around ~10%).
    I wasn't able to improve further the time request, because it seems connected with the operation of modify the rows one by one
    """
    data = data.apply(lambda row: _find_previous_frameword(data[:row.name], row), axis=1)
    return data


def _find_previous_frameword(data, row):
    """
    Find the frameword correlated with the hit.
    If T_coarse is greater than half of it's range, it will take the last odd frameword, otherwise the last even frameword. Some variables are explicit for clarify their role
    """
    T_coarse = (row["T_coarse"])
    hit_word_number = (row["word_number"])
    last_framewords = data.loc[(data["word_type"] == "frame") & (data["word_number"] < hit_word_number)].tail(2)["frame_count"].values  # We need the last two framewords to search for the right one
    if row["word_type"] == "hit":
        if len(last_framewords) > 1:  # We need at least 2 frameword to be sure that we are doing the right assignment
            if last_framewords[0] % 2 == 0:
                if T_coarse < 0xFFFF/2:  # The frameword to assign depends by this value
                    row["frame_count"] = last_framewords[0]
                else:
                    row["frame_count"] = last_framewords[1]
            else:
                if T_coarse < 0xFFFF/2:
                    row["frame_count"] = last_framewords[1]
                else:
                    row["frame_count"] = last_framewords[0]
    return row


class Run_Data_TM(Run_Data_TL):
    """
    Subclassed derived from the triggerless-mode version. The parsing is different
    """
    def __init__(self, run_path):
        Run_Data_TL.__init__(self, run_path)

    def _parser(self, data):
        """
        Parser for TM data format
        """
        this_word = {}
        reversed_data = (list(reversed(data)))  # Swaps the byte order
        if (reversed_data[0] >> 5) == 6:  # Packet header
            this_word = {
                "word_type": "header",
            }

        if (reversed_data[0] >> 6) == 0:  # It's a hit
            this_word = {
                "word_type": "hit",
                "TIGER": (reversed_data[0] >> 3) & 0x7,
                "Channel": (reversed_data[1] >> 2) & 0x3F,
                "TAC": (reversed_data[1]) & 0x3,
                "T_coarse": (reversed_data[2] << 8 | reversed_data[3] | reversed_data[4] >> 8) & 0xFFFF,
                "E_coarse": (reversed_data[4] << 4 | reversed_data[5] >> 4) & 0x3FF,
                "T_fine": (reversed_data[5] << 6 | reversed_data[6] >> 2) & 0x3FF,
                "E_fine": (reversed_data[6] << 8 | reversed_data[7]) & 0x3FF

            }

        if (reversed_data[0] >> 5) == 7:  # Packet Trailer
            this_word = {
                "word_type": "trailer",
            }

        if (reversed_data[0] >> 4) == 4:  # Packet Trailer
            this_word = {
                "word_type": "UDP_SEQNO",
            }
        return this_word


if __name__ == "__main__":
    TL_run_7 = Run_Data_TL("/home/alb/corso_python/exam_project/data"+sep+"RUN_10")
