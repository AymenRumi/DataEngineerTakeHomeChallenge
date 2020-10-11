# Solution : Mistplay Data Engineer Take Home Challenge 


## Data Preprocessing Object - Python 



## Solution - Jupyter Notebook 
```python
from typing import Iterator

def fib(n: int) -> Iterator[int]:
    a, b = 0, 1
    while a < n:
        yield a
        a, b = b, a + b
```

## Data Analysis - R
