import numpy as np
import time
from collections import Counter

class MessageLogAnalyzer(object):
    """
    Outil d'analyse des messages des logs
        1. Analyse les fréquences verticales des mots
        2. Clusterise les logs en conséquence
    """

    def __init__(self, stop_words=['for', 'user', 'on', 'is',
                                   'from', 'of', 'to', '0', 'file']):
        self.stop_words = stop_words

    def fit_count(self, X):
        """
        Traduit une matrice de mot en une matrice des fréquences
        verticales des mots etstocke le dictionnaire associé

        param :
            X : matrice des mots des messages de chaque logs
        returns :
            X_count : matrice des fréquences verticales des mots
        stores :
            vocabulary : le dictionnaire permettant la traduction
        """

        start = time.time()

        self.n_logs, self.n_words = X.shape

        vocabulary = []
        X_count = np.zeros(X.shape)

        for column in range(self.n_words):

            words = X[:, column]

            word_count = Counter(words)  # à revoir ?

            word_count[None] = 0
            for stop_word in self.stop_words:
                word_count[stop_word] = 0

            count = np.vectorize(word_count.get)(words)

            X_count[:, column] = count
            vocabulary.append(word_count)

        self.vocabulary = vocabulary

        end = time.time()

        print('[Success] Vocabulary processed within %.2fs' % (end - start))

        return X_count

    
    def count(self, X):
        """
        Traduit une matrice de mots avec le dictionnaire précédent

        params :
            X : matrice des mots des messages de chaque logs
        returns :
            X_count : matrice des fréquences verticales des mots
        """

        X_count = np.zeros(X.shape)

        for column in range(self.n_words):

            words = X[:, column]

            count = np.vectorize(self.vocabulary[column].get)(words)

            X_count[:, column] = count

        return X_count

    
    def fit_clusterize(self, X, X_count):
        """
        Utilise les matrices de mots et de fréquences verticales
        pour clusteriser chaque log

        param :
            X : matrice des mots des messages de chaque logs
            X_count : matrice des fréquences verticales des mots
        returns :
            y : labels des clusters ("log keys")
        stores :
            clusters : le dictionnaire permettant la traduction
            à partir du premier mot et de la fréquence max des mots
        """
        start = time.time()

        y = np.empty(X.shape[0], object)

        max_X_count = X_count.max(axis=1)
        labels = np.unique(max_X_count)

        clusters = {}
        k=0
        for label in labels[::-1]:

            mask = np.array(max_X_count == label)

            if sum(mask) != label:
                
                try:
                    first_words = np.unique(X[mask, 0])
                except:
                    y[mask_new] = None

                for first_word in first_words:

                    first_word_mask = X[:, 0] == first_word
                    mask_new = [all(tup) for tup in zip(first_word_mask, mask)]

                    #log_keys = print_description(X[mask_new])  # à revoir
                    y[mask_new] = k
                    #clusters[(label, first_word)] = log_keys
                    k += 1

            else:

                #first_word = repr(np.unique(X[mask, 0]))
                #log_keys = print_description(X[mask])
                y[mask] = k
                k+=1
                #clusters[(label, first_word)] = log_keys

        self.clusters = clusters

        end = time.time()
        print('[Success] %d Logs reduced into %d Clusters within %.2fs' %
              (len(max_X_count), len(set(y)), end - start))

        return y

    
    def clusterize(self, X, X_count):
        """
        TODO : NE FONCTIONNE PAS ENCORE
        
        Clusterise des logs grace aux matrices des mots et des 
        fréqeunces verticales

        params :
            X : matrice des mots des messages de chaque logs
            X_count : matrice des fréquences verticales des mots
        returns :
            y : labels des clusters ("log keys")
        """
        y = np.empty(X.shape[0], object)
        max_X_count = X_count.max(axis=1)
        y = np.vectorize(self.clusters.get)(zip(max_X_count, X[:, 0]))

        return y


# fonctions de visualisation

def describe(X):

    log_keys={}
    variables={}

    n_words = X.shape[1]
    for column in range(n_words):
        words = X[:,column]

        if len(words[words != np.array(None)]) != 0:
            words[words == np.array(None)] = ""
        else:
            n_words = column
            break

        unique_words = np.unique(words)
        
        if len(unique_words) == 1:
            log_keys[column] = unique_words[0]
        else:
            variables[column] = unique_words

    return n_words, log_keys, variables

def print_description(X, ignore_variables=True):

    n_words, log_keys, variables = describe(X)

    structure = ['*']*n_words
    for k in log_keys.keys():
        structure[k] = log_keys[k]

    if not ignore_variables :
        for k in variables.keys():
            structure[k] = repr(set(variables[k]))

    return ' '.join(str(v) for v in structure)

def print_clusters(df_log):
    for k, (label, df_grouped) in enumerate(df_log.groupby('label')):
        print("\n## -- CLUSTER %d -- ##" % k)
        X = df_grouped.iloc[:,2:-1].values
        print("size = %d" % X.shape[0])
        print(print_description(X, ignore_variables=True))
        print(print_description(X, ignore_variables=False))