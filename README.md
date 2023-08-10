[![Python 3.8](https://img.shields.io/badge/python-3.8-blue.svg)](https://www.python.org/downloads/release/python-380/)
[![PyPI downloads month](https://img.shields.io/pypi/dm/snowflake-ai.svg)](https://pypi.python.org/pypi/snowflake-ai/)

Snowflake-AI
=======

*Snowflake_AI* is a Snowflake-centric **Enterprise AI/ML** framework with the tight integration of popular Python data science libraries, e.g., *Pandas*, *Scikit-Learn*, *Pytorch*, *MLFlow*, *Tensorflow*, etc. This project simplifies the process of integrating your company's Snowflake data securely with those popular libraries, making it easier to develop *Snowflake AI* applications leveraging *Snowpark* and other Snowflake ML capabilities to deploy machine learning models in the production environment following the standard MLOps process and enterprise security and governance. Currently, this framework supports OAuth (Authorization Code flow, Device Code flow, etc.) and SAML SSO Snowflake Connection, Streamlit and Notebook Apps development framework, build-in light-weigth MLOps pipelines and initial *Snowpandas* feature engineering APIs.


Installation
------

Use pip to install:

```bash
pip install snowflake-ai
```

or, use Poetry to install:

```bash
poetry add snowflake-ai
```


Configuration
-------

Copy the sample configuration file from the <installed lib dir>/snowflake_ai/conf directory and update the Snowflake connection parameters.

For example, you may do the following to find out where is the <installed lib dir>:

```bash
pip show snowflake-ai
```

While you are in <installed lib dir>, you may do:

```bash
mkdir -p ~/.snowflake_ai/conf
cp snowflake_ai/conf/app_config.toml ~/.snowflake_ai/conf/
```

Then, edit snowflake_ai/conf/app_config.toml with your Snowflake connection details.
Please consult your system admins for your company specific OAuth setup (Note: Azure AD has been fully tested).


Usage
------

To get started with Snowflake-AI, import StreamlitApp and DataFrameFactory from the library:

```python
from snowflake_ai.common import DataFrameFactory as DFF
from snowflake_ai.apps import StreamlitApp

```

Create a SnowConnect instance:

```python
app = StreamlitApp("streamlit_group.app_1")
connect = app.get_default_snow_connect()

```

Specify the table/view or sql you want to work with and create a DataFrame:

```python
tbl_nm = 'SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER'
sdf = DFF.create_df(tbl_nm, connect)
```

Display the contents of the DataFrame:

```python
sdf.show()
```

Currently, in addition to Streamlit app support, Jupyter notebook, Console app, and Base App are supported.

From here, you can use Snowflake-AI to preprocess and manipulate your data using additional Snowpark API, Pandas and Scikit-Learn, Pytorch, etc. to train machine learning models, and perform predictions. Stay tuned, as there are many features in the pipelines waiting to be released.

Enjoy building your next AI/ML project with Snowflake-AI!



Contribution & Development
------

We are always actively seeking contributors to help enhance our open-source project! Whether you're interested in coding, designing, or testing and documentation, your participation can make a valuable difference. Join us in shaping the future of this project and be part of a collaborative community.

It is recommended that you have *Poetry* installed on your system as it would simplify dependence management and code releases. You can install *Poetry* by following the instructions [here](https://python-poetry.org/docs/).

Next, clone [*this*](https://github.com/illuminairy-ai/snowflake-ai.git) repository and navigate to the project directory (assume you are in bash terminal):

```bash
git clone https://github.com/illuminairy-ai/snowflake-ai.git
cd snowflake-ai
```

Then, use Poetry to install all dependencies:

```bash
poetry install
```


License
------

This project is licensed under the Apache 2.0 License - see the [LICENSE](https://github.com/illuminairy-ai/snowflake-ai/blob/master/LICENSE.txt) file for details.
