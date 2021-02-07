from setuptools import setup

setup(name='SWatCh Utilities',
      version='0.1',
      description='Global Surface Water Chemistry (SWatCh) Database data cleaning utility package',
      author='Lobke Rotteveel',
      author_email='lobke@rotteveel.ca',
      packages=['swatch_utils'],
      install_requires=[
      	'cycler (>=0.10, <1.0)',
      	'datetime (>=4.3, <5.0)',
      	'kiwisolver (>=1.2, <2.0)',
      	'matplotlib (>=3.2, <4.0)',
      	'numpy (>=1.18, <2.0)',
      	'pandas (>=1.0, <2.0)',
      	'pyparsing (>=2.4, <3.0)',
      	'python_dateutil (>=2.8, <3.0)',
      	'pytz (==2019.3)',
      	'six (>=1.14, <2.0)',
      	'tqdm (>=4.45, <5.0)',
      	'xlrd (>=1.2, <2.0)',
      	'zope.interface (>=5.0, <6.0)',
      ],
     )