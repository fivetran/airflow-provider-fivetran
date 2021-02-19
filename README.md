# Apache Airflow Provider for Fivetran

**Experimental library as of February 2021! The Fivetran devrel team maintains this provider in an experimental state and does not guarantee ongoing support yet.**

An Airflow operator for [Fivetran](fivetran.com), a Python library for syncing data with connectors to destinations.

## Installation

Pre-requisites: An environment running `apache-airflow`.

```
pip install airflow-provider-fivetran
```


## Modules

[Fivetran Operator](./fivetran_provider/operators/fivetran.py): A base operator for Fivetran. Import into your DAG via: 

```
from fivetran_provider.operators.fivetran import FivetranOperator
```

## Examples

See the [**examples**](./fivetran_provider/examples) directory for an example DAG.

**This operator is in very early stages of development! Feel free to submit issues, PRs, or email the current authors at [devrel@fivetran.com](mailto:devrel@fivetran.com) for feedback. Thanks to [Pete DeJoy](https://github.com/petedejoy) and the [Astronomer.io](https://www.astronomer.io/) team for the support, and [Great Expectations](https://github.com/great-expectations/airflow-provider-great-expectations) for the inspiration.
