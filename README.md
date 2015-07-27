## Spark API Enhanced

Currently Spark does not have rich sets of API to handle different files and data source.  
It will be better to create a RDD with a custom record reader and the reader handles the different format and source. 
The reader should be a generic interface. When creating a RDD, an implementation of reader is provided so we can
have uniform API for a range of file formats and data sources.

The objective of the change is only to add API to handle demilited files. The source is for Saprk 1.3.1.

The example file is tab demilited text file. It has two columns, id and name as below.
 |:----- | -------:|
 | 1     |    alex |
 | 2     |     joe |
 | 3     |    jhon |

I have create a "delimitFile" to create a RDD. It has two new function "dmap" and "dfilter". I use the index
to access each coulmn for now. I will add the name access later.

```scala
val file = sc.delimitFile("/home/xxia/tab.txt")
val fd = file.dmap(1, x => x + "d")
fd.collect
// res1: Array[String] = Array(1   alexd, 2        joed, 3 jhond)
```

```scala
val file = sc.delimitFile("/home/xxia/tab.txt")
val fd = file.dfilter(0, x => x.toInt > 1)
fd.collect
//res2: Array[String] = Array(2   joe, 3  jhon)
```

That's it.

Alex
