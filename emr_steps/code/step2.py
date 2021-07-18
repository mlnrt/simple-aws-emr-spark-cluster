import nltk
import argparse
import logging
from nltk.corpus import stopwords
from pyspark.sql import SparkSession

english_stopwords = set(stopwords.words('english'))

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')


def get_non_stopwords(line):
    tokens = nltk.word_tokenize(line)
    return [x.lower() for x in tokens if x.isalpha() and x.lower() not in english_stopwords]


if __name__ == "__main__":
    # Get the EMR Step arguments
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--data_source_folder', help="The folder URI where data are saved, typically an S3 bucket.")
    parser.add_argument(
        '--output_uri', help="The URI where output is saved, typically an S3 bucket.")
    args = parser.parse_args()

    logger.info("Starting the Spark session")

    with SparkSession.builder.appName("SIADS 516").getOrCreate() as spark:

        sc = spark.sparkContext

        text = sc.textFile(args.data_source_folder + "/nytimes_news_articles.txt")

        logger.info("Read the data, starting the data analysis")

        # This Spark Job uses RDDs to count occurrence of non stopwords in the New York Times articles dataset
        # And order them in descending orders

        # 1. filters out blank lines
        step_1 = text.filter(lambda x: x != "")
        # 2. filters out lines starting with 'URL'
        step_2 = step_1.filter(lambda x: x[:3] != "URL")
        # 3. creates a single list (using flatMap) that applies the get_non_stopwords function to each line
        step_3 = step_2.flatMap(get_non_stopwords)
        # 4. converts each resulting line to a pairRDD with words as keys and values of 1
        step_4 = step_3.map(lambda x: (x, 1))
        # 5. reduces the resulting RDD by key, adding up all the 1s
        step_5 = step_4.reduceByKey(lambda accumulator, value: accumulator + value)
        # 7. sorts the resulting list by the counts, in descending order.
        step_6 = step_5.sortBy(lambda x: -x[1])

        logger.info("Completed all steps, writing the data")

        # Output the results
        step_6.saveAsTextFile(args.output_uri)
