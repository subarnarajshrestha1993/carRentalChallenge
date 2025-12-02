import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
# uv add pandas scikit-learn

df = pd.read_csv("spam_data.csv")
X,y = df['text'], df['label']
print(df.columns)

vectorizer = CountVectorizer()
X_vectorized = vectorizer.fit_transform(X)

X_train, X_test, y_train, y_test = train_test_split(
    X_vectorized, y, test_size = 0.2, random_state = 42
)

model = LogisticRegression()
model.fit(X_train, y_train)

print(f"Model Accuracy: {model.score(X_test, y_test):.2%}")

def check_inbox(message):
    msg_vector = vectorizer.transform([message])
    prediction = model.predict(msg_vector[0])

    status = "SPAM" if prediction ==1 else "INBOX"
    print(f"'{message}' --> {status}")

check_inbox("Are we still on for Meeting?")
check_inbox("Click here to win free money! 500$")
check_inbox("Can you complete this task immediately?")