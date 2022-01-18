# ONEDOT DATA ENGINEERING TASK



## Required Libraries and Setting up the Envioronment 

* Used python evironment(anaconda)

pip install libraries
* Pandas


* PySpark
  * Installation guide for pySpark [here](https://medium.com/tinghaochen/how-to-install-pyspark-locally-94501eefe421)

## How to run
In the same folder with supplier_car.json

```sh
python main.py
```

## Files

* Data Files : 
  * supplier_car.json : initial supplier data
  * Target Data.xlsx: target data
  * pre-processing.csv : pre-processed version of supplier data
  * normalisation.csv : normalized version of pre-processed data
  * extraction.csv : extracted version of normalised data
  * integration.csv : integrated version of extracted data (This is the final form of the data)


* Python files :
  * main.py : pipeline needed to be executed
  * Classes:
    * preprocessor.py : Class with functions for preprocessing
    * normalizer.py : Class with functions for normalizing
    * extractor.py : Class with functions for extraction
    * integrator.py : Class with functions for integration


* Presentation files :

 * data_flow_presentation.pdf : Data flow presentation and explanations. Answer of part 5 can be found in last 2 slides.


  

  
