# The Surface Water Chemistry Database (SWatCh)

The scripts within this repository were developed to compile a standardised global database of acidification-related surface water chemistry - the Surface Water Chemistry (SWatCh) database. SWatCh was developed to facilitate powerful and robust statistical analyses of global surface water chemistry by providing one standardised, openly available, high quality, and trans-boundary database. The manuscript and findings associated with the database created using the scripts within this repository are submitted to Earth Systems Science Data and are presently undergoing review.

SWatCh is a relational database consisting of three datasets (sample, site, and method information) and one ArcGIS shapefile (point file of sample site locations), cross-referenced by site and method identification codes. A limited version of SWatCh can be obtained from PANGAEA (https://pangaea.de/). The limited version of the database does not include the sites and samples from the United Nations GEMStat database due to publication restrictions. The full version of SWatCh can be created using the scripts within this repository which clean, format, and compile data obtained from the resources below.

This is my first released code on GitHub, so I would love to hear your feedback and suggestions so I can improve this and future projects. I will do my best to respond to your comments when I have time to do so.

**Data Sources**

- http://data.ec.gc.ca/data/substances/monitor/national-long-term-water-quality-monitoring-data/
- http://mcm.lternet.edu/power-search/data-set
- https://gemstat.org/custom-data-request/
- https://doi.pangaea.de/10.1594/PANGAEA.902360
- https://www.waterqualitydata.us/portal/
- https://www.eea.europa.eu/data-and-maps/data/waterbase-water-quality-2

## Getting Started

SWatCh scripts require swatch_utils to run; these are scripts which automate standardising and checking units, extracting sampling method data, standardising dataframe structures, and standardising data types. These tools must be installed prior to running the other scripts.

### Prerequisites

SWatCh was developed in Python 3.6.9 using the packages and versions listed within `requirements.txt`. I have made my best guess regarding the compatible versioning within the `setup.py`. If you find the scripts do not run, you can duplicate my developing environment using the `requirements.txt` file. Note: if you use `requirements.txt` to install the prerequisites, swatch_utils will not be installed, and must me installed manually afterwards.

Requirements can be installed via the `requirements.txt` as below:

```bash
pip install -r requirements.txt
```

Python 3.6.9 can be downloaded from https://www.python.org/.

### Installing

Download the SWatCh scripts and install them using the line below, executed from the folder which contains the scripts:

```bash
python3 -m pip install .
```

SWatCh processing scripts can now be used.

### Running the Scripts

After downloading the data from the links above and renaming the files as indicated in the cleaning scripts and installing swatch_utils, the scripts can be run.

Datasets which are downloaded from the sources listed above must be re-named following the instructions provided within the scripts themselves. Additionally, the `path` variable must be set to the directory containing the re-named datasets.

The scripts must be run in the following order:

* clean_sites_[dataset name].py
* clean_samples_[dataset name].py

Where [dataset name] is the name of one of the datasets. Run all of the scripts with the above naming convention before proceeding to run the final script below. The cleaned datasets must be located within the same folder as the `merge.py` script.

*  merge.py

## Contributing

Please read [CONTRIBUTING.md](https://gist.github.com/PurpleBooth/b24679402957c63ec426) for details on GitHub's code of conduct, and the process for submitting pull requests.

## Versioning

SWatCh uses [SemVer](http://semver.org/) for versioning. For the versions available, see the tags on this repository.

## Authors

### Scripts

* **Lobke Rotteveel** - *Author* - [LinkedIn]([linkedin.com/in/lobke-rotteveel](https://www.linkedin.com/in/lobke-rotteveel))
* **Franz Heubach** - *Co-Author* - [GitHub](https://github.com/FranzHeubach), [LinkedIn]([linkedin.com/in/franz-heubach](https://www.linkedin.com/in/franz-heubach))

### Manuscript

* **Lobke Rotteveel** - *Author* - [LinkedIn]([linkedin.com/in/lobke-rotteveel](https://www.linkedin.com/in/lobke-rotteveel))
* **Dr. Shannon Sterling** - *Co-author* - [LinkedIn]([linkedin.com/in/shannon-sterling-23436616](https://www.linkedin.com/in/shannon-sterling-23436616))

## License

This project is licensed under the Creative Commons Attribution-ShareAlike 4.0 International license, please see [CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/) for details.

## Disclaimer

While substantial efforts are made to eliminate errors from the SWatCh database, complete accuracy of the data and
metadata cannot be guaranteed. All data and metadata are made available "as is". Neither Lobke Rotteveel and Dr. Shannon M. Sterling nor their current or future affiliated institutions, including the Sterling Hydrology Research Group and Dalhousie University, can be held responsible for harms, damages, or other consequences resulting from the use or interpretation of information contained within the SWatCh database.
The SWatCh database is licensed under the Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International
License. To view a copy of this license, visit http://creativecommons.org/licenses/by-nc-sa/4.0/ or send a letter to Creative Commons, PO Box 1866, Mountain View, CA 94042, USA.
