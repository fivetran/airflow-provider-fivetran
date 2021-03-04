# Apache Airflow Provider for Fivetran

**Experimental library as of February 2021! The Fivetran devrel team maintains this provider in an experimental state and does not guarantee ongoing support yet.**

An Airflow operator for [Fivetran](https://fivetran.com). Fivetran automates your data pipeline, and Airflow automates your data processing.

## Installation

Pre-requisites: An environment running `apache-airflow`.

```
pip install airflow-provider-fivetran
```

## Modules

[Fivetran Operator](./airflow_provider_fivetran/operators/fivetran.py): A base operator for Fivetran. Import into your DAG via:

```
from airflow_provider_fivetran.operators.fivetran import FivetranOperator
```

## Examples

See the [**examples**](./airflow_provider_fivetran/examples) directory for an example DAG.

**This operator is in very early stages of development! Feel free to submit issues, PRs, or email the current authors at [devrel@fivetran.com](mailto:devrel@fivetran.com) for feedback.
