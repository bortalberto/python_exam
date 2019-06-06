import os
import glob2
import pandas as pd
import sys
import time

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
        self.mode = str(self.__class__).split("Run_Data_")[1][0:2]
        self.run_path = run_path

        try:
            self.load_dataframe()
        except:
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
            print ("SUBRUN {}".format(subrun_number))
            # For first, runs a couple of checks on the file
            if mode != self.mode:
                raise Exception("Wrong trigger mode (got {}), maybe you want to use the other mode?".format(mode))
            file_size = os.stat(hit_file).st_size # Reads the number of Bytes in the file
            if file_size%8 != 0:
                raise Exception("The file length is not a multiple of 8 Bytes, something bad happen saving the file")
            with open(hit_file, 'rb') as fi:
                word_number=0
                while True: # Reads all the file. If TIGER_word_raw is empty, the end of the file is reached, each event is parsed into a dictionary and put in the global list
                    TIGER_word_raw = fi.read(8)
                    if not TIGER_word_raw:
                        break
                    word_number+=1
                    event_word_dict = {
                        "word_number":  int(word_number),
                        "sub_run":      int(subrun_number),
                        "GEMROC":       int(gemroc_number),
                    }
                    event_word_dict.update(self._parser(TIGER_word_raw)) #Add the hit informations to the generic informations

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

        :param data: 8 byte data word
        :return:
        """
        this_word = {}
        reversed_data = (list(reversed(data)))  # Swaps the byte order
        if (reversed_data[0]>>5) == 1:  # It's a frameword
            this_word = {
                "word_type":    "frame",
                "TIGER":         (reversed_data[0]) & 0x7,
                "frame_count":  (reversed_data[4] << 9 | reversed_data[5] << 1 | reversed_data[6] >> 7) & 0xFFFF,
                "SEU_count":    (reversed_data[6] << 8 | reversed_data[7]) & 0x7FFF
            }

        if (reversed_data[0]>>5) == 0:  # It's a hit
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
        if (reversed_data[0]>>5) == 2:  # It's a countword
            # not used up to now
            raise Exception("Count word found, but they are not in use, maybe something went wrong in the data transmission or in the parsing")
        return this_word

    def assign_frameword_to_hit(self):
        """
        This function assign at each hit, the correspondent frameword. The "orphan" hits are deleted (the first few hits in every subrun)
        The data are dedividedn group by hardware and sub_run, the "aggregate" function apply the function "assign_frameword_group" to every group like it is a separate dataframe
        """
        self.hit_df = self.hit_df.groupby(["sub_run", "GEMROC"], as_index=False).apply(assign_frameword_group)
        self.hit_df = self.hit_df.groupby(["sub_run", "GEMROC"], as_index=False).apply(remove_orphan_words)
        self.hit_df = self.hit_df.reset_index()
        self.hit_df.drop(self.hit_df.columns[0:2], axis=1)

    def save_dataframe(self):
        """
        In order to speed up the operations, the data can be saved and loaded
        :return:
        """
        self.hit_df.to_pickle(self.run_path + sep + "pandas_df_save")

    def load_dataframe(self):
        """
        In order to speed up the operations, the data can be saved and loaded
        :return:
        """
        self.hit_df = pd.read_pickle(self.run_path + sep + "pandas_df_save")
def remove_orphan_words(data):
    """
    Remove the hit and the frameword unusefull for the beginning and the end of the data
    """
    data = data[data.frame_count.isnull() == False]
    # data.drop(data.columns[0], axis=1, inplace=True)
    return data[1:-1]

def assign_frameword_group(data):
    """
    This function exist only to apply the function  "_find_previous_frameword" to each row of the data frame
    :param data:
    :return:
    """
    data = data.apply(lambda row: _find_previous_frameword(data[:row.name], row), axis=1)
    return data
def _find_previous_frameword (data,row):
    """
    Find the frameword correlated with the hit.
    If T_coarse is greater than half of it's range, it will take the last odd frameword, otherwise the last even frameword.
    :param even:
    :return:
    # """
    T_coarse = (row["T_coarse"])
    hit_word_number = (row["word_number"])
    TIGER = row["TIGER"]
    last_framewords = data.loc[(data["word_type"] == "frame") & (data["word_number"] < hit_word_number)].tail(2)["frame_count"].values
    if row["word_type"] == "hit":
        if len(last_framewords) > 1:
            if last_framewords[0] % 2 == 0:
                if T_coarse<0xFFFF/2:
                    row["frame_count"] = last_framewords[0]
                else:
                    row["frame_count"] = last_framewords[1]
            else:
                if T_coarse<0xFFFF/2:
                    row["frame_count"] = last_framewords[1]
                else:
                    row["frame_count"] = last_framewords[0]
    return row


class Run_Data_TM(Run_Data_TL):
    def __init__(self, run_path):
        Run_Data_TL.__init__(self, run_path)

    def _parser(self, data):
        """
        Parser for TM data format
        :param data:
        :return:
        """
        this_word = {}
        reversed_data = (list(reversed(data)))  # Swaps the byte order

        if (reversed_data[0] >> 5) == 6:  # Packet header
            print ("Header")
            this_word = {
                "word_type": "header",
            }

        if (reversed_data[0] >> 6) == 0:  # It's a hit
            print ("HIT")
            this_word = {
                "word_type": "hit",
                "TIGER": (reversed_data[0]) & 0x7,
                "Channel": (reversed_data[1]) & 0x3F,
                "TAC": (reversed_data[2] >> 6) & 0x3,
                "T_coarse": (reversed_data[2] << 10 | reversed_data[3] << 2 | reversed_data[4] >> 6) & 0xFFFF,
                "E_coarse": (reversed_data[4] << 4 | reversed_data[5] >> 4) & 0x3FF,
                "T_fine": (reversed_data[5] << 6 | reversed_data[6] >> 2) & 0x3FF,
                "E_fine": (reversed_data[6] << 8 | reversed_data[7]) & 0x3FF

            }

        if (reversed_data[0] >> 5) == 7:  # Packet Trailer
            print ("Trailer")
            this_word = {
                "word_type": "trailer",
            }

        if (reversed_data[0] >> 4) == 4:  # Packet Trailer
            print ("UDP_SEQNO")
            this_word = {
                "word_type": "UDP_SEQNO",
            }
        return this_word
if __name__ == "__main__":
    TL_run_7 = Run_Data_TL("/home/alb/corso_python/exam_project/data"+sep+"RUN_10")