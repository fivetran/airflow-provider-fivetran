# ⚠️ Archived

# Planned Deprecation Notice
We plan on deprecating this project in favor of [Astronomer's excellent async operator](https://github.com/astronomer/airflow-provider-fivetran-async). Their provider offers multiple advanced features that [we have written about here](https://www.fivetran.com/blog/introducing-the-new-fivetran-async-provider-in-airflow). 
  
In order to make this transition as seamless as possible, Astronomer has put together a [detailed guide on utilizing their provider](https://docs.astronomer.io/learn/airflow-fivetran).
  
## Why we're making this change
Astronomer has built a superior provider taking advantage of things like async orchestration via deferred sensors. Rather than split our resources, we've decided to join in on their effort to better support Fivetran customers that are utilizing Airflow. 
  
## What this means for you
Although we plan to deprecate this project, it doesn't mean it will suddenly disappear. However, we don't plan to provide further updates or support. We strongly recommend prioritizing a transition to Astronomer's provider to receive updates, bug fixes, and new features. 
  
## How to transition
Please refer to Astronomer's thoughtful [step-by-step guide on utilizing their provider](https://docs.astronomer.io/learn/airflow-fivetran). If you have any additional questions, please reach out to [Fivetran support](https://support.fivetran.com/hc/en-us) or your account manager. Thank you!

<br />
<br />
<br />

## Deprecated documentation bellow

# Fivetran Provider for Apache Airflow

This package provides an operator, sensor, and hook that integrates [Fivetran](https://fivetran.com) into Apache Airflow. `FivetranOperator` allows you to start Fivetran jobs from Airflow and `FivetranSensor` allows you to monitor a Fivetran sync job for completion before running downstream processes.

[Fivetran automates your data pipeline, and Airflow automates your data processing.](https://www.youtube.com/watch?v=siSx6L2ckSw&ab_channel=Fivetran)

## Installation

Prerequisites: An environment running `apache-airflow`.

```
pip install airflow-provider-fivetran
```

## Configuration

In the Airflow user interface, configure a Connection for Fivetran. Most of the Connection config fields will be left blank. Configure the following fields:

* `Conn Id`: `fivetran_default`
* `Conn Type`: `Fivetran`
* `Fivetran API Key`: Your Fivetran API Key
* `Fivetran API Secret`: Your Fivetran API Secret

Find the Fivetran API Key and Secret in the [Fivetran Account Settings](https://fivetran.com/account/settings), under the **API Config** section. See our documentation for more information on [Fivetran API Authentication](https://fivetran.com/docs/rest-api/getting-started#authentication).

The sensor and operator assume the `Conn Id` is set to `fivetran_default`, however if you are managing multipe Fivetran accounts, you can set this to anything you like. See the DAG in examples to see how to specify a custom `Conn Id`.

## Modules

### [Fivetran Operator](https://github.com/fivetran/airflow-provider-fivetran/blob/main/fivetran_provider/operators/fivetran.py)

`FivetranOperator` starts a Fivetran sync job. Note that when a Fivetran sync job is controlled via an Operator, it is no longer run on the schedule as managed by Fivetran. In other words, it is now scheduled only from Airflow.

`FivetranOperator` requires that you specify the `connector_id` of the sync job to start. You can find `connector_id` in the Settings page of the connector you configured in the [Fivetran dashboard](https://fivetran.com/dashboard/connectors).

Import into your DAG via:
```
from fivetran_provider.operators.fivetran import FivetranOperator
```

### [Fivetran Sensor](https://github.com/fivetran/airflow-provider-fivetran/blob/main/fivetran_provider/sensors/fivetran.py)

`FivetranSensor` monitors a Fivetran sync job for completion. Monitoring with `FivetranSensor` allows you to trigger downstream processes only when the Fivetran sync jobs have completed, ensuring data consistency. You can use multiple instances of `FivetranSensor` to monitor multiple Fivetran connectors.

Note, it is possible to monitor a sync that is scheduled and managed from Fivetran; in other words, you can use `FivetranSensor` without using `FivetranOperator`. If used in this way, your DAG will wait until the sync job starts on its Fivetran-controlled schedule and then completes.

`FivetranSensor` requires that you specify the `connector_id` of the sync job to start. You can find `connector_id` in the Settings page of the connector you configured in the [Fivetran dashboard](https://fivetran.com/dashboard/connectors).

Import into your DAG via:
```
from fivetran_provider.sensors.fivetran import FivetranSensor
```

## Examples

See the [**examples**](https://github.com/fivetran/airflow-provider-fivetran/tree/main/fivetran_provider/example_dags) directory for an example DAG.

## Issues

Please submit [issues](https://github.com/fivetran/airflow-provider-fivetran/issues) and [pull requests](https://github.com/fivetran/airflow-provider-fivetran/pulls) in our official repo:
[https://github.com/fivetran/airflow-provider-fivetran](https://github.com/fivetran/airflow-provider-fivetran)

We are happy to hear from you. Please email any feedback to the authors at [devrel@fivetran.com](mailto:devrel@fivetran.com).


## Acknowledgements

Special thanks to [Pete DeJoy](https://github.com/petedejoy), [Plinio Guzman](https://github.com/pgzmnk), and [David Koenitzer](https://github.com/sunkickr) of [Astronomer.io](https://www.astronomer.io/) for their contributions and support in getting this provider off the ground.
