### Chaining multiple transformers:
Multiple times while trying to use more than one transformation, I was required to chain up Transformers or build a pipeline.

Although pipeline was a go to way. Sometime it became overloaded while experimenting with different transformations.

Ex: Maintaining intermediate transformation columns and passing column names between transformers.

Thus I built a [Transform](https://github.com/AnkushKhanna/spark-common/blob/master/src/main/scala/common/transfomration/Transform.scala#L7-L14) class
which was extended by the most common transformers I used, Tokenizer, Hashing, TFIDF.

Thus now to chain Tokenizer and Hashing, we can use:
``` 
val transform = new Transform with TTokenize with THashing
```
To add TFIDF the transformer would look like:
```
val transform = new Transform with TTokenize with THashing with TIDF
```
This works from left to right. So first Tokenizer would be applied then Hashing and at last TFIDF. 

This make life easier while trying out different Transformer combination, without the headache of maintaining intermediate columns.

[See source code](https://github.com/AnkushKhanna/spark-common/blob/master/src/main/scala/common/transfomration/Transform.scala)

[See usage code](https://github.com/AnkushKhanna/spark-common/blob/master/src/test/scala/common/transfomration/TransformTest.scala)

To extend this class with further transformations, you can check out [Source code extension](https://github.com/AnkushKhanna/spark-common/blob/master/src/main/scala/common/transfomration/Transform.scala#L27-L38)
