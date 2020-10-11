# Solution : Mistplay Data Engineer Take Home Challenge 


## Data Preprocessing Object - Python 
[DataPrepocessor.py](https://github.com/AymenRumi/DataEngineerTakeHomeChallenge/blob/master/Solution/DataPrepocessor.py) contains code for the data processing object used for [Mistplay Data Engineering Challenge](https://github.com/Mistplay/DataEngineerTakeHomeChallenge).

This `object` will be called to process data according to Mistplay requirements.
This object may be used to process any dataset that adhere to proper file format(JSON & Parquet).

Futher methods may be added to deal with future data processing needs

Code below contains the `template` for the preprocessor object

```python

# Template for Data Preprocessing Object

class DataPrepocessor:
        
  def __init__():            # initializes object 
                                
  def importData():          # import data into object
  
  def printColumnNames():    # print data columns         
  
  def countDuplicates():     # count duplicate data entries in a given column      
  
  def removeDuplicates():    # remove duplicate entries in a given column
      
  def addRank():             # add rank for numerical column
  
  def anonymizeColumn():     # encrypt a column
  
  def decryptColumn():       # decrypt an encrypted column 
  
  def decryptEntry():        # decrypt ecnrypted data entry
  
  def invertedIndex():       # create inverted index table
  
  def saveData():            # save dataset
  
  def JSONtoParquet():       # convert JSON to parquet
  
  def getEncryptionKeys():   # return encryption keys for encrypted columns
  
  def getData():             # return data

```

The [code](https://github.com/AymenRumi/DataEngineerTakeHomeChallenge/blob/master/Solution/DataPrepocessor.py) follows `object oriented design` standard and is meant to be continuously used for future data preprocessing.

With focus on:
1. `Maintainability`
3. `Reusability`
2. `Scalability` (PySpark Dataframe needed for Big Data Processing)
               
## Solution - Jupyter Notebook 
```python
from DataPrepocessor import DataPrepocessor

preprocessor=DataPrepocessor("https://raw.githubusercontent.com/Mistplay/DataEngineerTakeHomeChallenge/master/data.json")
```

## Data Analysis - R
