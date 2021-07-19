from nltk.sentiment import SentimentIntensityAnalyzer

def sentance_analyze(sent):
    score = SentimentIntensityAnalyzer()
    return score.polarity_scores(f"{sent}")


if __name__ == '__main__':
    word = input("Enter the sentence:")
    out = sentance_analyze(word)
    print(f"Score: {out}")