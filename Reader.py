from glob import glob
import numpy as np
# Classe qui permet de lire le contenu de fichiers text


class Reader:

    def read_file(self, file_path):
        """
        Open text file and return list of lines
        """
        results = []
        with open(file_path, newline='') as inputfile:
            for row in inputfile:
                results.append(row.replace('\n', '').replace('\t', ''))
        return results

    def read_dir(self, dir_path, nb_files_max=np.inf):
        """
        read_file for every file in dir, group result in one list
        """
        res = []
        p = 0
        for filename in glob(dir_path):
            if p > nb_files_max:
                break
            print(filename)
            res += self.read_file(filename)
            p += 1

        print('\n[Success] %d files loaded' % (p-1))

        return res
