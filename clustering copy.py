from sentence_transformers import SentenceTransformer
from sklearn.cluster import MiniBatchKMeans
import pandas as pd
from sentimental import connect_elasticsearch
import time

# The model maps sentences & paragraphs to a 384 dimensional dense
model = SentenceTransformer('paraphrase-MiniLM-L6-v2')

#Our sentences we like to encode
sentences = ['This framework generates embeddings for each input sentence fsd s ddskf kjhj h  fjh fjld shgj hjkfghsjkd ghjksfhg kjsfhgjk shdgjk hfjlg hls ghjksf hgljk sfhdgj hslfj ghjfsd hgljf dhgljshfgjlkfhgjkl hsdfghf jlhkjfh gkjf shdgjk fhjl ghfslgj pre ogjklsfngsuhgirojgrgoksfk ',
    'Sentences are passed as a list of string.',
    'The quick brown fox jumps over the lazy dog.']

#Sentences are encoded by calling model.encode()
embeddings = model.encode(sentences)

# print(embeddings.shape)

# kmeans = KMeans(n_clusters=18, random_state=0).fit(embeddings)
# Y=kmeans.labels_
# z = pd.DataFrame(Y.tolist())

if __name__ == "__main__":

    es = connect_elasticsearch()
    starttime = time.time()
    kmeans = MiniBatchKMeans(n_clusters=2,
                                random_state=0)
    # Sleep for 30 seconds
    time.sleep(30.0 - ((time.time() - starttime) % 30.0))
    response = es.search(index="tweet", query={
    "bool": {
      "filter": {
        "range": {
          "date": {
            "gte": "now-30m",
            "lt": "now"
                }
            }
        }
    }
    })
    total = response['hits']['total']['value']
    data = response['hits']["hits"]
    got = len(data)
    assert total == got

    kmeans = kmeans.partial_fit(X[0:6,:])
    kmeans = kmeans.partial_fit(X[6:12,:])


