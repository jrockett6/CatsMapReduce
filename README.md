# CatsMapReduce
Purely functional, multithreaded, map-reduce implementation on top of cats, cats-effect, and fs2.

https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf

### To run the code

Developed with Java 8 (Open JDK 1.8) and sbt version 1.3.13

Clone...

Navigate to folder:

`cd path/to/CatsMapReduce`

Run MapReduce:

`sbt compile 'run --f testdata_1.txt --n 2'`, where `--f` indicates desired input file, and `--n` is the desired
number of reducers (and output files).

and test your result:

`python src/test/test-result.py resources/input/testdata_2.txt resources/result/` 

### TODO
- Currently `Reducer` uses `java.io.FileOutputStream` for writing intermediate files to results. I would like to
 create a solution that won't memory overflow for large (intermediate) files. Currently `Mapper` reads/writes
  shards -> intermediate files using `fs2` in 4096 byte chunks. Which avoids the large file/small mem problem for the
   original files. However to avoid this problem for `Reducer` (with the intermediate files) it's necessary to have
   some form of external sort algorithm (which I would like to implement on `fs2`) in the `Shuffler` to appropriately
    group (and sort) similar keys.
    
- The current command line parsing far from exhaustive in terms of input handling and error verbosity, it should be
 implemented with something like: http://ben.kirw.in/decline/
 
- One goal of this project was to better understand cats-effect threading, so the mappers and reducers currently just
 run in threadpools with Cats Effect context switching. However, I would like to rewrite this in a more realistic 
 multi-process fashion with remote communication.
 
- I'd like to implement a proper concurrent queue for `map` and `reduce` jobs to then be able to include worker-failure 
handling, timeouts, and racing on parallel jobs.

- Clearly there is much more that goes into a proper MR program, cluster management considerations such as fault-tolerance, 
backup tasks, network locality, etc. These are (likely) beyond the scope of this project.
 
