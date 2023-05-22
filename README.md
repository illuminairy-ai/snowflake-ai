[![Python 3.8](https://img.shields.io/badge/python-3.8-blue.svg)](https://www.python.org/downloads/release/python-380/)

Snowflake-AI
=======

*Snowflake_AI* is a Snowflake-centric **Enterprise AI/ML** framework with tight integration of popular Python data science libraries, e.g., *Pandas*, *Scikit-Learn*, *Tensorflow*, *Pytorch*, *MLFlow*, etc. This project simplifies the process of integrating your company's Snowflake data with those popular libraries, making it easier to develop and deploy machine learning models. Currently, this support OAuth and SAML SSO Snowflake Connection, Streamlit Apps development framework and initial Snowpandas.

Installation
------

Before getting started, ensure you have *Poetry* installed on your system. You can install Poetry by following the instructions [here](https://python-poetry.org/docs/).

Next, clone [*this*](https://github.com/tonyxliu/snowflake-ai.git) repository and navigate to the project directory (assume you are in bash terminal):

```bash
git clone https://github.com/tonyxliu/snowflake-ai.git
cd snowflake-ai
```

Then, use Poetry to install all dependencies:

```bash
poetry install
```

Configuration
-------

Copy the sample configuration file and update the Snowflake connection parameters:

```bash
mkdir -p ~/snowflake_ai/conf
cp snowflake_ai/conf/app_config.toml ~/snowflake_ai/conf/
```

Edit snowflake_ai/conf/app_config.toml with your Snowflake connection details.

Usage
------

To get started with Snowflake-AI, import SnowConnect and DataFrameFactory from the library:

```python
from snowflake_ai.common import SnowConnect
from snowflake_ai.common import DataFrameFactory
```

Create a SnowConnect instance:

```python
connect = SnowConnect()
```

Specify the table/view or sql you want to work with and create a DataFrame:

```python
tbl_nm = 'SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER'
sdf = DataFrameFactory.create_df(tbl_nm, connect)
```

Display the contents of the DataFrame:

```python
sdf.show()
```

From here, you can use Snowflake-AI to preprocess and manipulate your data using Pandas and Scikit-Learn, train machine learning models, and perform predictions. Stay tuned, as there are many features in the pipelines waiting to be released.

Enjoy building your next AI/ML project with Snowflake-AI!

License
------

This project is licensed under the BSD 3-Clause License - see the [LICENSE](https://github.com/tonyxliu/snowflake-ai/blob/master/LICENSE) file for details.
