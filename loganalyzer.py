import numpy as np
import time
from collections import Counter


class MessageLogAnalyzer(object):
    """
    Outil d'analyse des messages des logs
        1. Analyse les fréquences verticales des mots
        2. Clusterise les logs en conséquence
    """

    def __init__(self, stop_words=True):

        if stop_words == True:
            self.stop_words = frozenset([
                "a", "about", "above", "across", "after", "afterwards", "again", "against",
                "all", "almost", "alone", "along", "already", "also", "although", "always",
                "am", "among", "amongst", "amoungst", "amount", "an", "and", "another",
                "any", "anyhow", "anyone", "anything", "anyway", "anywhere", "are",
                "around", "as", "at", "back", "be", "became", "because", "become",
                "becomes", "becoming", "been", "before", "beforehand", "behind", "being",
                "below", "beside", "besides", "between", "beyond", "bill", "both",
                "bottom", "but", "by", "call", "can", "cannot", "cant", "co", "con",
                "could", "couldnt", "cry", "de", "describe", "detail", "do", "done",
                "down", "due", "during", "each", "eg", "eight", "either", "eleven", "else",
                "elsewhere", "empty", "enough", "etc", "even", "ever", "every", "everyone",
                "everything", "everywhere", "except", "few", "fifteen", "fifty", "fill",
                "find", "fire", "first", "five", "for", "former", "formerly", "forty",
                "found", "four", "from", "front", "full", "further", "get", "give", "go",
                "had", "has", "hasnt", "have", "he", "hence", "her", "here", "hereafter",
                "hereby", "herein", "hereupon", "hers", "herself", "him", "himself", "his",
                "how", "however", "hundred", "i", "ie", "if", "in", "inc", "indeed",
                "interest", "into", "is", "it", "its", "itself", "keep", "last", "latter",
                "latterly", "least", "less", "ltd", "made", "many", "may", "me",
                "meanwhile", "might", "mill", "mine", "more", "moreover", "most", "mostly",
                "move", "much", "must", "my", "myself", "name", "namely", "neither",
                "never", "nevertheless", "next", "nine", "no", "nobody", "none", "noone",
                "nor", "not", "nothing", "now", "nowhere", "of", "off", "often", "on",
                "once", "one", "only", "onto", "or", "other", "others", "otherwise", "our",
                "ours", "ourselves", "out", "over", "own", "part", "per", "perhaps",
                "please", "put", "rather", "re", "same", "see", "seem", "seemed",
                "seeming", "seems", "serious", "several", "she", "should", "show", "side",
                "since", "sincere", "six", "sixty", "so", "some", "somehow", "someone",
                "something", "sometime", "sometimes", "somewhere", "still", "such",
                "system", "take", "ten", "than", "that", "the", "their", "them",
                "themselves", "then", "thence", "there", "thereafter", "thereby",
                "therefore", "therein", "thereupon", "these", "they", "thick", "thin",
                "third", "this", "those", "though", "three", "through", "throughout",
                "thru", "thus", "to", "together", "too", "top", "toward", "towards",
                "twelve", "twenty", "two", "un", "under", "until", "up", "upon", "us",
                "very", "via", "was", "we", "well", "were", "what", "whatever", "when",
                "whence", "whenever", "where", "whereafter", "whereas", "whereby",
                "wherein", "whereupon", "wherever", "whether", "which", "while", "whither",
                "who", "whoever", "whole", "whom", "whose", "why", "will", "with",
                "within", "without", "would", "yet", "you", "your", "yours", "yourself",
                "yourselves", "0", "1", "2", "3", "4", "5", "6", "7", "8", "9"])

        else:
            self.stop_words = []

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

        self.n_words = X.shape[1]

        vocabulary = []
        X_count = np.zeros(X.shape)

        for column in range(self.n_words):

            words = X[:, column]
            word_count = Counter(words)

            word_count[None] = 0
            for stop_word in self.stop_words:
                word_count[stop_word] = 0

            count = np.vectorize(word_count.get)(words)

            X_count[:, column] = count
            vocabulary.append(dict(word_count))

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

        start = time.time()

        X_count = np.zeros(X.shape)

        for column in range(X.shape[1]):

            words = X[:, column]

            count = []

            for word in words:
                try:
                    # if word in self.vocabulary[column]:
                    count.append(self.vocabulary[column][word])
                except (IndexError, KeyError):
                    count.append(0)

            X_count[:, column] = count

        end = time.time()

        print('[Success] Vocabulary processed within %.2fs' % (end - start))

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
        argmax_X_count = X_count.argmax(axis=1)
        labels = np.unique(max_X_count)

        clusters = {}
        k = 0
        for label in labels[::-1]:

            mask = np.array(max_X_count == label)

            for position in np.unique(argmax_X_count):

                new_mask = mask & np.array(
                    argmax_X_count == position)  # à optimiser

                words = X[new_mask, position]
                words = words[words != np.array(None)]
                words = np.unique(words)

                for word in words:

                    word_mask = np.array(X[:, position] == word)
                    last_mask = new_mask & word_mask

                    log_keys = print_description(X[last_mask])  # à revoir
                    y[last_mask] = log_keys

                    clusters[(int(label), position, word)] = {
                        'log_keys': log_keys, 'size': sum(last_mask)}

        self.clusters = clusters

        end = time.time()

        print('[Success] %d Logs reduced into %d Clusters within %.2fs' %
              (len(max_X_count), len(set(y)), end - start))

        return y

    def clusterize(self, X, X_count):
        """
        Clusterise des logs grace aux matrices des mots et des 
        fréqeunces verticales

        params :
            X : matrice des mots des messages de chaque logs
            X_count : matrice des fréquences verticales des mots
        returns :
            y : labels des clusters ("log keys")
        """
        start = time.time()

        y = np.empty(X.shape[0], object)

        max_X_count = X_count.max(axis=1)
        argmax_X_count = X_count.argmax(axis=1)

        for i, line in enumerate(X):
            if (int(max_X_count[i]), argmax_X_count[i], line[argmax_X_count[i]]) in self.clusters:
                y[i] = self.clusters[(
                    int(max_X_count[i]), argmax_X_count[i], line[argmax_X_count[i]])]['log_keys']
            else:
                y[i] = -1  # outlier

        end = time.time()

        print('[Success] %d Logs reduced into %d Clusters within %.2fs' %
              (len(max_X_count), len(set(y)), end - start))

        #y = np.vectorize(self.clusters.get)(zip(max_X_count, X[:, 0]))

        return y


# fonctions de visualisation

def describe(X):

    log_keys = {}
    variables = {}

    n_words = X.shape[1]
    for column in range(n_words):
        words = X[:, column]

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

    if not ignore_variables:
        for k in variables.keys():
            structure[k] = repr(set(variables[k]))

    return ' '.join(str(v) for v in structure)


def print_clusters(df_log):
    for k, (label, df_grouped) in enumerate(df_log.groupby('label')):
        print("\n## -- CLUSTER %d -- ##" % k)
        X = df_grouped.iloc[:, 2:-1].values
        print("size = %d" % X.shape[0])
        print(print_description(X, ignore_variables=True))
        #print(print_description(X, ignore_variables=False))
