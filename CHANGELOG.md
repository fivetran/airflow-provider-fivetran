# Apache Airflow Provider for Fivetran

## Upcoming 
* (please add here)

## 1.1.2 - 2022-07-08
Added functionality to start initial syncs and syncs on paused connectors via the FivetranOperator 

## [1.1.1](https://github.com/fivetran/airflow-provider-fivetran/compare/v1.1.0...v1.1.1) - 2022-06-21
Fixed timestamp comparisons in `start_fivetran_sync` to be compare between pendulum.datetime.datetime instead of comparing strings

## [1.1.0](https://github.com/fivetran/airflow-provider-fivetran/releases/tag/v1.1.0) - 2022-06-15
Added functionality to ensure proper FivetranSensor montioring by passing timestamp from FivetranOperator via XCOM

Changed return value of FivetranOperator
Before 1.1.0:
```
{'code': 'Success',
 'message': "Sync has been successfully triggered for connector with id 'declivity_crisped'"}
```

New return value is timestamp of previously completed sync as a string:
```
2022-06-15T03:26:22.369239Z
```

## 1.0.3
Updated scheduling options, manipulate connector's schedule in FivetranOperator with `schedule_type` as `auto` or `manual`


## 1.0.2
Added parameter to FivetranOperator for connector schedule management, to use set `manual` to `true` or `false`

## [1.0.1](https://github.com/fivetran/airflow-provider-fivetran/releases/tag/v1.0.1) - 2021-04-17
Updating provider with additional DAG examples, fixing issues

## 1.0.0
First major release!

