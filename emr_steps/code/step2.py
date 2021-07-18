import nltk
import argparse
import re
import logging
from pyspark.sql import SparkSession

#nltk.download('book')
#nltk.download('punkt')
#nltk.download('averaged_perceptron_tagger')

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

patterns = """
    NP: {<JJ>*<NN*>+}
    {<JJ>*<NN*><CC>*<NN*>+}
    """

TOKEN_RE = re.compile(r"\b[\w']+\b")

NPChunker = nltk.RegexpParser(patterns)


def prepare_text(input):
    sentences = nltk.sent_tokenize(input)
    sentences = [nltk.word_tokenize(sent) for sent in sentences]
    sentences = [nltk.pos_tag(sent) for sent in sentences]
    sentences = [NPChunker.parse(sent) for sent in sentences]
    return sentences


def parsed_text_to_NP(sentences):
    nps = []
    for sent in sentences:
        tree = NPChunker.parse(sent)
        for subtree in tree.subtrees():
            if subtree.label() == 'NP':
                t = subtree
                t = ' '.join(word for word, tag in t.leaves())
                nps.append(t)
    return nps


def sent_parse(input):
    sentences = prepare_text(str(input))
    nps = parsed_text_to_NP(sentences)
    return nps


def pos_tag_counter(line):
    toks = nltk.regexp_tokenize(line, TOKEN_RE)
    postoks = nltk.tag.pos_tag(toks)
    return postoks


if __name__ == "__main__":
    # Get the EMR Step arguments
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--data_source_folder', help="The folder URI where data are saved, typically an S3 bucket.")
    parser.add_argument(
        '--output_uri', help="The URI where output is saved, typically an S3 bucket.")
    args = parser.parse_args()

    logger.info("Starting the Spark session")

    with SparkSession.builder.appName("SIADS 516 Homework 2").getOrCreate() as spark:

        sc = spark.sparkContext

        text = sc.textFile(args.data_source_folder + "/nytimes_news_articles.txt")

        logger.info("Read the data, starting the data analysis")

        # 1. filters out blank lines
        step_1 = text.filter(lambda x: x != "")
        # 2. filters out lines starting with 'URL'
        step_2 = step_1.filter(lambda x: x[:3] != "URL")
        # 3. creates a single list (using flatMap) that applies the pos_tag_counter function to each line
        step_3 = step_2.flatMap(pos_tag_counter)
        # 4. maps each resulting line to show the part of speech (which is the second element returned
        # from the pos_tag_counter)
        step_4 = step_3.map(lambda x: x[1])
        # 5. converts each resulting line to a pairRDD with POS tags as keys and values of 1
        step_5 = step_4.map(lambda x: (x, 1))
        # 6. reduces the resulting RDD by key, adding up all the 1s (like the lecture and lab examples)
        step_6 = step_5.reduceByKey(lambda accumulator, value: accumulator + value)
        # 7. sorts the resulting list by the counts, in descending order.
        step_7 = step_6.sortBy(lambda x: -x[1])

        logger.info("Completed all steps, writing the data")

        # Output the results
        step_7.saveAsTextFile(args.output_uri)
