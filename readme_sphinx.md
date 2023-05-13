Followed these: 
- https://betterprogramming.pub/auto-documenting-a-python-project-using-sphinx-8878f9ddc6e9
- https://stackoverflow.com/questions/53668052/sphinx-cannot-find-my-python-files-says-no-module-named
- https://python.plainenglish.io/how-to-host-your-sphinx-documentation-on-github-550254f325ae

Following are the steps:

1. Create and change directory to docs
```commandline
mkdir docs
cd docs
```

2. Run sphinx-quickstart
```commandline
sphinx-quickstart
```

3. Change `conf.py` as follows:
```Python
# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

import os
import sys
basedir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, basedir)


# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'VMT-Mix Development for Emission Inventories'
copyright = '2023, Emissions and Energy Modeling Team'
author = 'Emissions and Energy Modeling Team'
release = '0.0.1'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = ["sphinx.ext.napoleon", "sphinx.ext.autodoc"]

templates_path = ['_templates']
exclude_patterns = []



# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'sphinx_rtd_theme'
html_static_path = ['_static']
```

4. Edit index.rst as follows:
```text
.. toctree::
   :maxdepth: 2
   :caption: Description of my CodeBase:

   modules
```

5. Run 
```commandline
sphinx-apidoc -o ./source ..
```
6. Run
```commandline
make html
```