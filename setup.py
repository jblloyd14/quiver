import setuptools


setuptools.setup(
    name="quiver",
    version="0.0.5",
    author="some guy",
    author_email="@finblocks.com",
    description="a library for managing and querying time series data",
    url="",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)