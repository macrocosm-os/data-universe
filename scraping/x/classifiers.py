from transformers import AutoTokenizer
from transformers import AutoModelForSequenceClassification, TFAutoModelForSequenceClassification
import torch
import numpy as np
from scipy.special import expit
from typing import List
from langdetect import detect
from langdetect.lang_detect_exception import LangDetectException

class TweetLabeler:
    def __init__(self, model_name: str = "cardiffnlp/tweet-topic-latest-multi"):
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.model = AutoModelForSequenceClassification.from_pretrained(model_name)
        self.class_mapping = self.model.config.id2label
        self.model_name = model_name


    def check_english(self, text: str) -> bool:
        if not text:
            return False

        try:
            if detect(text) != "en":
                return False
        except LangDetectException:
            return False

        return True


    def label_tweet_singular(self, text: str) -> str:
        """ Returns best possible tweet label based on text """
        # currently using label_tweet_multiple
        return self.label_tweet_multiple(text)[0]


    def label_tweet_multiple(self, text: str) -> List[str]:
        """ Returns a list of possible tweet labels from highest to lowest score, above 0.5"""
        tokens = self.tokenizer(text, return_tensors = 'pt') # Pytorch
        output = self.model(**tokens)
        scores = output[0][0].detach().numpy()
        scores = expit(scores)
        label_predictions = (scores >= 0.5) * 1

        # mapping to classes
        label_list = []
        for i in range(len(label_predictions)):
            if label_predictions[i]:
                label_list.append(f"{self.class_mapping[i]}")

        return label_list
        



