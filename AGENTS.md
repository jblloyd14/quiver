## Purpose:
- Quiver is a datastore primarily for financial timeseries data, taking advantage of polars for high performance input/output

## package management
- first try 
  - conda install pkg_name
- then try
  - pip install pkg_name

## primary technologies used
- python >= 3.11
- pandas >= 2.2.2
- polars >= 1.12
- pyarrow >= 16.1.0
- duckdb == 1.12
- numpy == 1.16.4

## Coding Conventions
- Use reStructuredText format for all docstrings
  - :param name: type: description
## examples
- for example uses see /README.md

## when stuck
- ask a clarifying question, propose a short plan

## Project structure
```
├── AGENTS.md
├── pyproject.toml
├── quiver
│   ├── config.py
│   ├── __init__.py
│   ├── item.py
│   ├── library.py
│   ├── quiver_mgr
│   │   ├── data_explore
│   │   │   └── old_data_check.ipynb
│   │   ├── load_raw_data
│   │   │   └── spiderrock.ipynb
│   │   ├── polars testing.ipynb
│   │   ├── pystore examples.ipynb
│   │   ├── quiver_mgmt.ipynb
│   │   ├── quiver_testing.ipynb
│   │   └── sec_maps.ipynb
│   ├── subject.py
│   ├── Untitled Folder
│   └── utils.py
├── README.md
└── setup.py
```

## Don't
- push any changes without confirmation