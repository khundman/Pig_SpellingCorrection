jobs = load 'jobstest/jobtest.txt' using PigStorage(',') as (jobID, description);
dict = load 'testdict.txt' as dictwords;
stop = load 'stopwords/stopwords-en.txt' as stopwords;
flatjob = foreach jobs generate jobID, flatten(TOKENIZE(description)) as jobwords;
cgrp = COGROUP flatjob by jobwords, stop by stopwords;
noStops = FILTER cgrp BY IsEmpty($2.stopwords);
noStops_flat = foreach noStops generate flatten($1);
register /home/huser19/jobs/STEM.jar;
stemmed = foreach noStops_flat generate $0 as jobID, STEM($1) as wordstem;
cgrp2 = COGROUP stemmed by wordstem, dict by dictwords;
temp = FILTER cgrp2 by NOT IsEmpty ($2.dictwords);
temp2 = FILTER temp by NOT IsEmpty ($1.wordstem);
correct = foreach temp2 generate flatten($1);
temp3 = FILTER cgrp2 by IsEmpty ($2.dictwords);
temp4 = FILTER temp3 by NOT IsEmpty ($1.wordstem);
incorrect = foreach temp4 generate flatten($1) as (JobID, misspelled);
incorrectWord = GROUP incorrect by misspelled;
incorrectWord2 = foreach incorrectWord generate group;
crossed = CROSS incorrectWord2, dict;
register /home/huser19/levjob/LEV.jar;
distance = foreach crossed generate $0 as misspelled, $1 as replacement, LEV(group, dictwords) as distance;
corrected = GROUP distance by misspelled;
shortestA = foreach corrected generate group, $1 as together, MIN($1.distance) as minimum;
flattened = foreach shortestA generate group, flatten(together), minimum;
shortestB = FILTER flattened by $3 == $4;
finalCorrected = foreach shortestB generate $0, $2;
almostFinished = join incorrect by misspelled, finalCorrected by $0;
closer = foreach almostFinished generate $0 as JobID, $3 as fixed;
evenCloser = GROUP closer by JobID;
finished = foreach evenCloser generate group, $1.fixed;





