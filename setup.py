import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="snowflake-ai",
    version="0.5.5",
    author="Illuminairy AI",
    author_email="tony.liu@yahoo.com",
    description="A Snowflake centic Enterprise AI/ML framework with tight integration of popular data science libraries",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/illuminairy-ai/snowflake-ai",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.10",
        "License :: OSI Approved :: Apache Software License", 
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
        "Topic :: Software Development :: Libraries :: Python Modules"
    ],
)