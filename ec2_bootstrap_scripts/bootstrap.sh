#!/bin/bash
sudo python3 -m pip install boto3 nltk scikit-learn pandas
sudo mkdir /home/nltk_data
sudo python3 -m nltk.downloader -d /home/nltk_data book
sudo python3 -m nltk.downloader -d /home/nltk_data punkt
sudo python3 -m nltk.downloader -d /home/nltk_data averaged_perceptron_tagger