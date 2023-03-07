___
# S3 Data Processing   
___

## Problem:
A directory in S3 contains files with two columns

> 1. The files contain headers, the column names are just random strings and they are not consistent across files.  
> 2. both columns are integer values.  
> 3. Some files are CSV some are TSV - so they are mixed, and you must handle this.  
> 4. The empty string should represent 0.  
> 5. Henceforth the first column will be referred to as the key, and the second column the value.  
> 6. For any given key across the entire dataset (so all the files), there exists exactly one value that occurs an odd number of times.  

> E.g. you will see data like this:  
>> value 2 occurs odd number of times  
1 -> 2  
1 -> 3  
1 -> 3  

>> value 4 occurs odd number of times  
2 -> 4  
2 -> 4  
2 -> 4

> But never like this:  
>> three values occur odd number of times   
1 -> 2   
1 -> 3   
1 -> 4

>> no value for this key occurs odd number of times   
2 -> 4   
2 -> 4

> Write an app in Scala that takes 3 parameters:  
>>* An S3 path (input).   
>>* An S3 path (output).   
>>* An AWS ~/.aws/credentials profile to initialise creds with, or param to indicate that creds should use default provider chain. Your app will assume these creds have full access to the S3 bucket.     

> Then in spark local mode the app should write file(s) to the output S3 path in TSV format with two columns such that -  
>>* The first column contains each key exactly once.  
>>* The second column contains the integer occurring an odd number of times for the key, as described by #6 above.

---

### Input

### AWS Credentials Profile
Here's an example AWS credentials profile that you can add to your ~/.aws/credentials file.

```
[default]
aws_access_key_id = YOUR_ACCESS_KEY_ID
aws_secret_access_key = YOUR_SECRET_ACCESS_KEY
```

Replace YOUR_ACCESS_KEY_ID and YOUR_SECRET_ACCESS_KEY with your actual AWS access key and secret key, respectively. 
Then, in this app, you can specify this profile by passing "default" as the third argument to main method. 
You can create different aws profile as per your requirement and pass one as the argument.

###  Sample input path for args 0
Here is an example S3 output path that we can use for testing this app.
```
s3://my-bucket/my-directory/
```

Replace my-bucket and my-directory with the actual name of S3 bucket and directory, respectively. 
Make sure that your directory contains at least one file with the format described in the prompt.

###  Sample input path for args 1
Here is an example S3 output path that we can use for testing this app.
```
s3://my-bucket/my-output-directory/
```

Replace my-bucket and my-output-directory with the actual name of your S3 bucket and output directory, respectively. 
Note that the output directory does not need to exist in advance, This app will create it if it doesn't already exist. 
Also, make sure that you have write permission for the S3 bucket and directory that you specify as the output path.

###  Sample input path for args 2
Here is an example of how you should specify the default credentials provider chain as the third argument to this main method.
```
"default"
```

If you have the AWS CLI installed and have run aws configure, then the default provider chain will automatically use the
credentials you configured with aws configure. If you're running your app on an EC2 instance with an IAM role attached, 
the default provider chain will automatically use the IAM role's credentials.

### Sample Testing Data
Here are examples of CSV and TSV input files that we can use for testing our app.

CSV File (test.csv):  

```
col1,col2
1,2
1,3
1,3
2,4
2,4
2,4
```

TSV File (test.tsv):

```
col1    col2
1       2
1       3
1       3
2       4
2       4
2       4
```

Note that the files should be placed in the S3 directory that you specified as the input path when running your app. 
Also, make sure that the values in each file are separated by the correct delimiter (, for CSV and \t for TSV).

---

### How to run?
### On Local using AWS Credentials:

You can run the S3FileProcessing app on your local machine by setting the master configuration property to "local" 
before running the app. This will tell Spark to run in local mode instead of connecting to a cluster. 
Here's how you can modify the main method in the S3FileProcessing app to run in local mode:

```
val spark = SparkSession.builder()
  .appName("S3FileProcessing")
  .master("local[*]") // set the master configuration property to "local"
  .getOrCreate()
```

With this change, you can run the app on your local machine and it will read data from an S3 bucket, process it using Spark, 
and write the output back to an S3 bucket. Note that you'll still need to provide valid S3 input and output paths, 
as well as valid AWS credentials or a valid AWS credentials profile, even if you're running the app locally.

### On Local using Local File System
You can modify the S3FileProcessing app to read input data from a local file system instead of an S3 bucket by changing 
the input path argument to a local file path. Here's an modification you can make to the main method to read input data 
from a local file system:

```
val inputPath = "file:///path/to/local/directory/*" // set the input path to a local file path
val df = spark.read.format("csv").option("header", "true").option("delimiter", "\t").load(inputPath)
```

With this change, the spark.read method will read input data from the local file system instead of an S3 bucket. 
You'll need to replace /path/to/local/directory/* with the actual path to the local directory that contains your input files.

Similarly, you can modify the output path argument to specify an output path on the local file system. Here's an example 
modification to write the output data to a local file system:

```
val outputPath = "/path/to/local/output/directory" // set the output path to a local file path
result.write.format("csv").option("header", "true").option("delimiter", "\t").save(outputPath)
```

With this change, the result.write method will write the output data to the local file system instead of an S3 bucket. 
You'll need to replace /path/to/local/output/directory with the actual path to the local directory where you want to save 
your output files. Note that the output directory should not exist before running the app. The app will create it for you 
if it doesn't already exist.

---

#### Bonus 1:  
Provide more than one implementation of the algorithm, and as comments discuss the time & space complexity.

#### Time Complexity:
* Reading input files:    
    The time complexity for reading input files is dependent on the size of the input data and the number of input files. 
    Since the application reads input data using Apache Spark, it can process data in parallel, which can improve performance. 
    Therefore, the time complexity for reading input files can be considered ***O(n)***, where n is the size of the input data.   


* Processing input data:   
    The time complexity for processing input data depends on the number of rows in the input data. 
    Since the application groups the input data by key and sums the values for each key, the time complexity for processing 
    input data can be considered ***O(m log m)***, where m is the number of unique keys in the input data.

   
* Writing output data:    
    The time complexity for writing output data is dependent on the size of the output data and the number of output files. 
    Similar to reading input data, the application can write output data in parallel, which can improve performance. 
    Therefore, the time complexity for writing output data can be considered ***O(p)***, where p is the size of the output data.


* Overall, the time complexity of the S3FileProcessing Spark application can be considered ***O(n + m log m + p)***.

#### Space Complexity:
* Reading input files:    
    The space complexity for reading input files is dependent on the size of the input data and the number of input files. 
    Since the application reads input data using Apache Spark, it can process data in parallel and load only a subset of 
    the data into memory at any given time. Therefore, the space complexity for reading input files can be considered ***O(1)***.   


* Processing input data:    
    The space complexity for processing input data depends on the number of unique keys in the input data. 
    Since the application groups the input data by key and sums the values for each key, it needs to keep track of each 
    unique key and its corresponding total value in memory. Therefore, the space complexity for processing input data can be considered ***O(m)***.


* Writing output data: 
    The space complexity for writing output data is dependent on the size of the output data and the number of output files. 
    Since the application writes output data using Apache Spark, it can write data in parallel and write only a subset of the 
    data to disk at any given time. Therefore, the space complexity for writing output data can be considered ***O(1)***.


* Overall, the space complexity of the S3FileProcessing Spark application can be considered ***O(m)***.

---





