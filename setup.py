import setuptools

setuptools.setup(
    name="job-run-loop",
    version="0.0.1",
    author="Stijn Rosaer",
    author_email="stijn.rosaer@telenet.be",
    description="Python package for job fetching",
    python_requires=">=3.7",
    install_requires=[
        "flask",
        "requests",
        "SPARQLWrapper",
        "rdflib",
    ],
    dependency_links=[
        "git+https://github.com/stijnrosaer/mu-python-sparql-helper.git#egg=mu_sparql_helpers",
    ],
    url="https://github.com/stijnrosaer/job-run-loop",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)