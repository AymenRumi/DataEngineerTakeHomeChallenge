# Solution : Mistplay Data Engineer Take Home Challenge 


## Data Preprocessing Object - Python 
[DataPrepocessor.py](https://github.com/AymenRumi/DataEngineerTakeHomeChallenge/blob/master/Solution/DataPrepocessor.py) contains code for a data processing object used for Mistplay Data Engineering Challenge.

This Object will be called to process data according to Mistplay requirements
This object may be used to process any dataset that adhere to proper file format. Futher methods may be added to deal with future data processing needs

```python

# Template for Data Preprocessing Object

class DataPrepocessor:
        
  def __init__():      # initializes object 
                                
  def importData():    # import data into object
  
  def printColumnNames():    # print data columns         
  
  def countDuplicates():     # count duplicate data entries in a given column      
  
  def removeDuplicates():    # remove duplicate entries in a given column
      
  def addRank():         # add rank for numerical column
  
  def anonymizeColumn():     # encrypt a column
  
  def decryptColumn():       # decrypt an encrypted column 
  
  def decryptEntry():        # decrypt ecnrypted data entry
  
  def invertedIndex():       # create inverted index table
  
  def saveData():         # save dataset
  
  def JSONtoParquet():        # convert JSON to parquet
  
  def getEncryptionKeys():    # return encryption keys for encrypted columns
  
  def getData():          # return data

```
## Solution - Jupyter Notebook 
```python
from DataPrepocessor import DataPrepocessor

preprocessor=DataPrepocessor("https://raw.githubusercontent.com/Mistplay/DataEngineerTakeHomeChallenge/master/data.json")
```

## Data Analysis - R
