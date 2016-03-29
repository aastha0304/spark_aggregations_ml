# baggins
Repository for zeo Hack Lab, where data is the ring and we are Bilbo Baggins, precious! 

This project aims to accomplish what zHL aims to accomplish, which includes all this
- There's a stream of bidrequests from some users coming from a non-zeotap ssp.
- There's a stream of bidrequests for same users coming from zeotap ssp. Zeotap adds users' segment information to this stream. This means, users' gender, age, income group.
- For privacy sake and for preventing identification, zeotap anonymises certain bid attributes. 
- Everybody wants this rich user information for which they pay zeotap some money, but they dont want to pay that money.
- So somebody might try to fingerprint zeotap's bidstream and use that to match against bidstream from non-zeotap ssp.
- If they are successful, they have user segment information from zeotap and also unique attributes from other ssps; so they know more about the user than the users' mother.
- We don't want that to happen.

So first of all, prove, is it really possible to match these 2?

Yes :(

How do we know it?

We did some things on the modified bid logs and found that the original ids present in these modified bid logs (last column in tap separated lines) are matching the ones in original logs.


What things did you do?

The code in this project uses spark for everything. It starts by taking, for first 2 commandline arguments, modified bidstream and original bidstream. It runs a spark MR to
- filter original bidstream on carrier (since modified bidstream is only for 1 carrier, we know that). That ways we can really bring down the volume for lookup
- create key value where key is a combination of {first 2 octets of ip, timestamp, city, region} and value is the whole bid request object for both streams
- join the 2 bidstreams based on same key, so that the match has to be done only inside smaller subsets rather than on the superset 
- this join is actually a collection of many modified bid requests against many original bid requests, so flatten to make single modified bidrequest against one or more originals
- if the above join creates a 1:1 mapping for some cases, save these cases right here , they are done.
- for the others, the match is 1:n, check for closest match in this 'n'. For closest, find similarity score.
- Similarity score is based on fuzzy string match of string attributes, and variance for integral attributes.
- find highest similarity score in this 1:m system. 
- if it is >0.65, call it a match
- done this for all sets, means found probable matches for all sets. 
- save the results, check against raw bid logs.

Ok, may be we should obfuscate some features without reducing bid request value. What features are most telling of this information?

Filtering is based on time and IP; apart from these, UA, BCAT etc are a factor.

How do you know that?

We ran a feature analysis.

How did you do that?

We converted the unsupervised problem into a supervised one with binomial classes and ran logistic regression on it. That gave us coefficients for the features used. 

Wow, how did you do that?

- steps 1-4 are same as above.
- if the above join creates a 1:1 match, get similarity score as done in step 7 above.
- if the score is above 0.65, label will be 1 or positive class; else 0 or negative class)
- if the match is 1:n, get highest and second highest similarity score in the set.
- if the highest is above 0.65 and the difference between highest and second is >0.25, label will be 1 for highest match, for others it will be 0
- this way we create our binomial labels. 
- now we need to use the features. We can't put the string features as our columns, there are just too many, for example, user agents, blocked advertising lists etc. 
- so the features are the fuzzy distance between 2 strings for string features, 0 or 1 for categorical features, and continuous values for numerical features.
Eg
for 2 requests
schema
id, user agent(string), tmax(int), is_dnt(boolean)
modified request:
request_tid, mozilla firefox 2.3 samsung android, 285, 1, 
original request
request_id, mozilla firefox 2 samsung galaxy, 278, 0
and if score was 0.72
the feature row is
0.94,0.9,0 
there is no similarity matching for ids so only 3 columns in feature vector
This makes an input line in data set as
[(1), 0.94, 0.9, 0]
- Feed this input to logistic regression
- split into traing and test set and run regression
- get the coefficients for the features.
- interpret them , the higher the more contributing

Why do you think that by taking the difference between similarity scores will help with correct identification ?

Even though we have original ids, another DSP won't have it. So we are trying to guess what another DSP will do without this information. Also, this seems to work for this guy 
http://blog.kaggle.com/2015/09/16/icdm-winners-interview-3rd-place-roberto-diaz/

Why do you think similarity value will work, instead of actual attribute value for feature set in logit?

If we create a sparse vector based on categories, it won't solve the problem of feature relevance of column, it will solve for that value of the column. So we don't make n-1 encoding or anything.

You are not smart enough to pull this of. Who helped you?

- this guy, 
http://blog.kaggle.com/2015/09/16/icdm-winners-interview-3rd-place-roberto-diaz/
is trying to solve a similar problem
- https://www.kaggle.com/c/icdm-2015-drawbridge-cross-device-connections
- Also, the book Programming Collective Intelligence, is pretty good. The matchmaker chapter helped with logic for logit.
- Wikipedia is the best source for understanding logit
https://en.wikipedia.org/wiki/Logistic_regression
- Or, Andrew NG
- Coding help came from 
- https://spark.apache.org/docs/latest/ml-classification-regression.html
- http://spark.apache.org/docs/latest/mllib-linear-methods.html













