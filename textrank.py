"""
From this paper: https://web.eecs.umich.edu/~mihalcea/papers/mihalcea.emnlp04.pdf

External dependencies: nltk, numpy, networkx

Based on https://gist.github.com/voidfiles/1646117
"""

import io
import nltk
import itertools
from operator import itemgetter
import networkx as nx
import os


keywordNum = 30

#apply syntactic filters based on POS tags
def filter_for_tags(tagged, tags=['NN', 'NNP']):
    return [item for item in tagged if item[1] in tags]

def normalize(tagged):
    return [(item[0].replace('.', ''), item[1]) for item in tagged]

def unique_everseen(iterable, key=None):
    "List unique elements, preserving order. Remember all elements ever seen."
    seen = set()
    seen_add = seen.add
    if key is None:
        try:
            for element in itertools.ifilterfalse(seen.__contains__, iterable):
                seen_add(element)
                yield element
        except:
            for element in iterable:
                if element not in seen:
                    seen_add(element)
                    yield element

    else:
        for element in iterable:
            k = key(element)
            if k not in seen:
                seen_add(k)
                yield element

def lDistance(firstString, secondString):
    "Function to find the Levenshtein distance between two words/sentences - gotten from http://rosettacode.org/wiki/Levenshtein_distance#Python"
    if len(firstString) > len(secondString):
        firstString, secondString = secondString, firstString
    distances = range(len(firstString) + 1)
    for index2, char2 in enumerate(secondString):
        newDistances = [index2 + 1]
        for index1, char1 in enumerate(firstString):
            if char1 == char2:
                newDistances.append(distances[index1])
            else:
                newDistances.append(1 + min((distances[index1], distances[index1+1], newDistances[-1])))
        distances = newDistances
    return distances[-1]

def buildGraph(nodes):
    "nodes - list of hashables that represents the nodes of the graph"
    gr = nx.Graph() #initialize an undirected graph
    gr.add_nodes_from(nodes)
    nodePairs = list(itertools.combinations(nodes, 2))

    #add edges to the graph (weighted by Levenshtein distance)
    for pair in nodePairs:
        firstString = pair[0]
        secondString = pair[1]
        levDistance = lDistance(firstString, secondString)
        gr.add_edge(firstString, secondString, weight=levDistance)

    return gr

def extractKeyphrases(text, keyWordNum):
    #tokenize the text using nltk
    wordTokens = nltk.word_tokenize(text)

    #assign POS tags to the words in the text
    tagged = nltk.pos_tag(wordTokens)
    textlist = [x[0] for x in tagged]

    tagged = filter_for_tags(tagged)
    tagged = normalize(tagged)

    unique_word_set = unique_everseen([x[0] for x in tagged])
    #unique_word_set = unique_everseen([x[0] for x in tagged], str.lower)
    word_set_list = list(unique_word_set)

   #this will be used to determine adjacent words in order to construct keyphrases with two words

    if len(word_set_list) > 800:
        return []
    graph = buildGraph(word_set_list)

    #pageRank - initial value of 1.0, error tolerance of 0,0001,
    calculated_page_rank = nx.pagerank(graph, weight='weight')

    #most important words in ascending order of importance
    keyphrases = sorted(calculated_page_rank, key=calculated_page_rank.get, reverse=True)

    #the number of keyphrases returned will be relative to the size of the text (a third of the number of vertices)
    #aThird = int(len(word_set_list) / 3)
    #keyphrases = keyphrases[0:aThird+1]
    keyphrases = keyphrases[0:keyWordNum+1]
    return keyphrases

def extractSentences(text):
    sent_detector = nltk.data.load('tokenizers/punkt/english.pickle')
    sentenceTokens = sent_detector.tokenize(text.strip())
    graph = buildGraph(sentenceTokens)

    calculated_page_rank = nx.pagerank(graph, weight='weight')

    #most important sentences in ascending order of importance
    sentences = sorted(calculated_page_rank, key=calculated_page_rank.get, reverse=True)

    #return a 100 word summary
    summary = ' '.join(sentences)
    summaryWords = summary.split()
    summaryWords = summaryWords[0:101]
    summary = ' '.join(summaryWords)

    return summary

def writeFiles(summary, keyphrases, fileName):
    "outputs the keyphrases and summaries to appropriate files"
    print( "Generating output to " + 'keywords/' + fileName)
    keyphraseFile = io.open('keywords/' + fileName, 'w')
    for keyphrase in keyphrases:
        keyphraseFile.write(keyphrase + '\n')
    keyphraseFile.close()

    print( "Generating output to " + 'summaries/' + fileName)
    summaryFile = io.open('summaries/' + fileName, 'w')
    summaryFile.write(summary)
    summaryFile.close()
    print( "-")


if __name__=="__main__":
    s = """And just like always, another company turns their back on their customer base that grew their IP to be as successful as it is. I can't even play Pokemon Go simply because I don't have a phone. Why the fuck wouldn't nintendo make it work for 3DS? Oh yeah, because they're stupid just like every other dumb company who only cares about money. Screw you all, release PKMN Go for 3DS or I am selling my 3DS because I don't even use it and don't care about it. Moronic."""
    keyphrases = extractKeyphrases(s, 30)
    print(keyphrases)

