# EMF
Repository for Open Source EMF implementation proof of concept


# Business Process
## Overview
![RSC EMF protsess-FUTURE drawio](https://user-images.githubusercontent.com/11408965/210733092-81fdea1e-f6c2-4df9-b9ab-e340d4847897.svg)
## IGM Validation
Incoming IGM-s need to be validated by EMF tool:
 - To ensure that the IGM-s can be imported to the tool
 - It is possible to solve the LoadFlow
 - To provide feedback for TSO-s on QAS portal
 - To check and store the AC NP and HVDC flows data

IGM validation process should run continuously:
 - To give TSO-s continuous and timely feedback on their IGM, witch enables them to update or fix IGM before the merging process starts
 - To achieve better performance during the merging process, as all IGM-s would be already validated by that time


## IGM Replacement
If IGM is deemed unusable:
 - TSO can either send a new IGM before the gate closure or
 - RCC can use replacement logic to use an older IGM from different timestamp or process

IGM replacement logic is defined in EMF requirements document.

_TODO - That logic should be put to a configuration file._

## CGM creation

Purpose is to create one electrical grid model from all available models for a given Scenario Time

### Data Collection and Import

 - Retrieve all valid IGM-s (including replaced IGM-s) for given merge type and scenario time
 - Retrieve the latest valid BDS (Boundary Set)
 - Retrieve all reference schedules values for given scenario time
 - Import all files to Load Flow tool
 - Switch off or set to 0 matched cim:EquivalentInjection-s on matched borders
 - Proceed to Scaling

### Scaling
 - Scale the Loads to match each Area AC NP to the reference program (the current state is recorded during IGM Validation)
 - Set the HVDC flows to match reference program
 - Run fist LoadFlow
 - Continue Scaling Loads until desired mismatch achieved


- _TODO: copy from EMF dock the exact process_
- _TODO: describe when to use relaxed PF settings_


## Relevant references
 - EMF Requirements
 - EMF Assessment
 - Quality of Datasets

_TODO: add links_


# Logical Components
## Overview
![RSC EMF protsess-FUTURE drawio](https://user-images.githubusercontent.com/11408965/210733092-81fdea1e-f6c2-4df9-b9ab-e340d4847897.svg)
## LoadFlow Tool
 - Must be able to import CIM XML files (EQ, SSH, TP, SV)
 - Able to use CIM LoadFlow settings
 - Export CIM Merged model (N x SSH, SV)

## Process Configurator
 - Store and Edit Process configurations
 - Store and Edit Time Frame configurations

### Process configuration 

- _TODO: add description tables_
- _TODO: generate schemas_
- _TODO: add to example LF settings_

#### Example:
```json
{
  "@context": "https://example.com/schemas/process.jsonld",
  "@id": "https://example.com/processes/CGM_CREATION", 
  "@type": "Process",
  "description": "", 
  "time_zone": "Europe/Brussels", 
  "tags": [],
  "properties": {},
  "runs": [
    {
      "@context": "https://example.com/schemas/run.jsonld",
      "@id": "https://example.com/runs/IntraDayCGM",
      "@type": "Run",
      "process_id": "https://example.com/processes/CGM_CREATION",
      "valid_from": "",
      "valid_to": "",
      "gate_open": "PT1H",
      "gate_close": "PT45M",
      "run_at": "05 07,15,23 * * *",
      "time_frame": "H-8", 
      "tags": [],
      "properties": {
        "data_timestamps": "30 * * * *",
        "data_resolution": "PT1H",
        "merge_type": "CGM", 
        "time_horizon": "ID"
      }
    },
    {
      "@context": "https://example.com/schemas/run.jsonld",
      "@id": "https://example.com/runs/DayAheadCGM",
      "@type": "Run",        
      "process_id": "https://example.com/processes/CGM_CREATION",
      "valid_from": "",
      "valid_to": "",
      "gate_open": "PT6H",
      "gate_close": "PT5H",
      "run_at": "50 18 * * *",
      "time_frame": "D-1",
      "properties": {
        "data_timestamps": "30 * * * *",
        "data_resolution": "PT1H",
        "merge_type": "CGM",
        "time_horizon": "1D"
      }
    },
    {
      "@context": "https://example.com/schemas/run.jsonld", 
      "@id": "https://example.com/runs/TwoDaysAheadCGM",
      "@type": "Run", 
      "process_id": "https://example.com/processes/CGM_CREATION",
      "valid_from": "",
      "valid_to": "",
      "gate_open": "P1DT5H",
      "gate_close": "P1DT4H",
      "run_at": "50 19 * * *",
      "time_frame": "D-2",
      "properties": {
        "data_timestamps": "30 * * * *",
        "data_resolution": "PT1H",
        "merge_type": "CGM"
      }
    }
  ]
}
```

#### Schema

- _TODO: Needs review and corrections_
```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "$id": "https://example.com/schemas/process.json",
  "title": "Process",
  "description": "Schema for describing a process.",
  "type": "object",
  "properties": {
    "@context": {
      "type": "string",
      "const": "https://example.com/schemas/process.jsonld"
    },
    "@id": {
      "type": "string",
      "format": "uri"
    },
    "@type": {
      "type": "string",
      "const": "Process"
    },
    "description": {
      "type": "string"
    },
    "time_zone": {
      "type": "string"
    },
    "tags": {
      "type": "array",
      "items": {
        "type": "string"
      }
    },
    "properties": {
      "type": "object"
    },
    "runs": {
      "type": "array",
      "items": {
        "type": "object",
        "properties": {
          "@context": {
            "type": "string",
            "const": "https://example.com/schemas/run.jsonld"
          },
          "@id": {
            "type": "string",
            "format": "uri"
          },
          "@type": {
            "type": "string",
            "const": "Run"
          },
          "process_id": {
            "type": "string",
            "format": "uri"
          },
          "valid_from": {
            "type": "string"
          },
          "valid_to": {
            "type": "string"
          },
          "gate_open": {
            "type": "string"
          },
          "gate_close": {
            "type": "string"
          },
          "run_at": {
            "type": "string"
          },
          "time_frame": {
            "type": "string"
          },
          "tags": {
            "type": "array",
            "items": {
              "type": "string"
            }
          },
          "properties": {
            "type": "object",
            "properties": {
              "data_timestamps": {
                "type": "string"
              },
              "data_resolution": {
                "type": "string"
              },
              "merge_type": {
                "type": "string"
              },
              "time_horizon": {
                "type": "string"
              }
            },
            "required": [
              "data_timestamps",
              "data_resolution",
              "merge_type"
            ]
          }
        },
        "required": [
          "@context",
          "@id",
          "@type",
          "process_id",
          "properties"
        ]
      }
    }
  },
  "required": [
    "@context",
    "@id",
    "@type",
    "runs"
  ]
}

```

### Time Frame configuration 

#### Example
```json
[
    {
        "@context": "https://example.com/timeHorizon_context.jsonld",
        "@id": "https://example.com/timeHorizons/ID",
        "@type": "https://example.com/timeHorizon",
        "description": "Process running continuously within given day",
        "period_duration": "P1D",
        "period_start": "P0D",
        "reference_time": "currentDayStart"
    },
    {
        "@context": "https://example.com/timeHorizon_context.jsonld",
        "@id": "https://example.com/timeHorizons/H-8",
        "@type": "https://example.com/timeHorizon",
        "description": "Process running 8 hours ahead in intraday",
        "period_duration": "PT8H",
        "period_start": "PT1H",
        "reference_time": "currentHourStart"
    },
    {
        "@context": "https://example.com/timeHorizon_context.jsonld",
        "@id": "https://example.com/timeHorizons/D-1",
        "@type": "https://example.com/timeHorizon",
        "description": "Process that runs day before the targeted day",
        "period_duration": "P1D",
        "period_start": "P1D",
        "reference_time": "currentDayStart"
    },
    {
        "@context": "https://example.com/timeHorizon_context.jsonld",
        "@id": "https://example.com/timeHorizons/D-2",
        "@type": "https://example.com/timeHorizon",
        "description": "Process that runs two days before the targeted day",
        "period_duration": "P1D",
        "period_start": "P2D",
        "reference_time": "currentDayStart"
    },
    {
        "@context": "https://example.com/timeHorizon_context.jsonld",
        "@id": "https://example.com/timeHorizons/D-7",
        "@type": "https://example.com/timeHorizon",
        "description": "Process that runs day before the targeted day and covers time window of 7 days",
        "period_duration": "P7D",
        "period_start": "P1D",
        "reference_time": "currentDayStart"
    },
    {
        "@context": "https://example.com/timeHorizon_context.jsonld",
        "@id": "https://example.com/timeHorizons/W-0",
        "@type": "https://example.com/timeHorizon",
        "description": "Process that runs for current week",
        "period_duration": "P1W",
        "period_start": "P0W",
        "reference_time": "currentWeekStart"
    },
    {
        "@context": "https://example.com/timeHorizon_context.jsonld",
        "@id": "https://example.com/timeHorizons/W-1",
        "@type": "https://example.com/timeHorizon",
        "description": "Process that runs in current week for next week",
        "period_duration": "P1W",
        "period_start": "P1W",
        "reference_time": "currentWeekStart"
    },
    {
        "@context": "https://example.com/timeHorizon_context.jsonld",
        "@id": "https://example.com/timeHorizons/M-1",
        "@type": "https://example.com/timeHorizon",
        "description": "Process that runs in current month for next month",
        "period_duration": "P1M",
        "period_start": "P1M",
        "reference_time": "currentMonthStart"
    },
    {
        "@context": "https://example.com/timeHorizon_context.jsonld",
        "@id": "https://example.com/timeHorizons/Y-1",
        "@type": "https://example.com/timeHorizon",
        "description": "Process that runs in current year for next year",
        "period_duration": "P1Y",
        "period_start": "P1Y",
        "reference_time": "currentYearStart"
    }
]
```

#### Schema

- _TODO: Needs review and corrections_
```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "$id": "https://example.com/timeHorizon_schema.json",
  "type": "array",
  "items": {
    "type": "object",
    "required": [
      "@context",
      "@id",
      "@type",
      "description",
      "period_duration",
      "period_start",
      "reference_time"
    ],
    "properties": {
      "@context": {
        "type": "string",
        "format": "uri"
      },
      "@id": {
        "type": "string",
        "format": "uri"
      },
      "@type": {
        "type": "string",
        "format": "uri"
      },
      "description": {
        "type": "string"
      },
      "period_duration": {
        "type": "string"
      },
      "period_start": {
        "type": "string"
      },
      "reference_time": {
        "type": "string"
      }
    }
  }
}
```

## Process Scheduler
 - Store Process configurations
 - Responsible for triggering automatic tasks
   - Check if there are any tasks to be run based on valid Process Configurations
   - Check if there are any event that should trigger a task to be run
 - Enable manual creation of Tasks
 - Publish Tasks to Service Bus



### Task Format

- _TODO: Add LF settings_
- _TODO: Add Reference Schedule_
- _TODO: Should replacement logic be linked here as well?_
- _TODO: Add description of the format and schema_

#### Example
```json
{
  "@context": "https://example.com/task_context.jsonld",
  "@type": "Task",
  "@id": "urn:uuid:<uuid4>",
  "process_id": "https://example.com/processes/CGM_CREATION",
  "run_id": "https://example.com/runs/DayAheadCGM",
  "task_type": "automatic",
  "task_initiator": "system_id_or_username",
  "task_priority": "high",
  "task_creation_time": "2023-03-04T19:30:00Z",
  "task_dependencies": [],
  "task_tags": [],
  "task_retry_count": 0,
  "task_timeout": "PT1H",
  "task_properties": {
    "merge_type": "CGM",
    "time_horizon": "1D",
    "scenario_time": "2023-03-05T16:30Z"
  }
}
```
- _TODO: Needs review and check, currently autogenerated from example_
#### Schema
```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "Task",
  "type": "object",
  "required": ["@context", "@type", "@id", "process_id", "run_id", "task_type", "task_initiator", "task_priority", "task_creation_time", "task_dependencies", "task_tags", "task_retry_count", "task_timeout", "task_properties"],
  "properties": {
    "@context": {
      "type": "string",
      "const": "https://example.com/task_context.jsonld"
    },
    "@type": {
      "type": "string",
      "const": "Task"
    },
    "@id": {
      "type": "string",
      "pattern": "^urn:uuid:[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[8|9|aA|bB][a-f0-9]{3}-[a-f0-9]{12}$"
    },
    "process_id": {
      "type": "string",
      "format": "uri"
    },
    "run_id": {
      "type": "string",
      "format": "uri"
    },
    "task_type": {
      "type": "string",
      "enum": ["automatic", "manual"]
    },
    "task_initiator": {
      "type": "string"
    },
    "task_priority": {
      "type": "string",
      "enum": ["high", "medium", "low"]
    },
    "task_creation_time": {
      "type": "string",
      "format": "date-time"
    },
    "task_dependencies": {
      "type": "array",
      "items": {
        "type": "string",
        "format": "uri"
      }
    },
    "task_tags": {
      "type": "array",
      "items": {
        "type": "string"
      }
    },
    "task_retry_count": {
      "type": "integer",
      "minimum": 0
    },
    "task_timeout": {
      "type": "string",
      "pattern": "^PT\\d+[HMS]$"
    },
    "task_properties": {
      "type": "object",
      "properties": {
        "merge_type": {
          "type": "string"
        },
        "time_horizon": {
          "type": "string"
        },
        "scenario_time": {
          "type": "string",
          "format": "date-time"
        }
      },
      "required": ["merge_type", "time_horizon"]
    }
  }
}

```


## Task Manager
 - Manage and monitor the task trough out the task life cycle

## Metadata and Data Storage
This system consists of two separate but integrates systems

 - Metadata Storage: 
   - Store all extracted metadata about Models
   - Store reference to original data stored in Object Storage
   - Store reference schedules as searchable documents per each timestamp in timeseries _TODO - Add example_
   - ELK stack is planned to be used
 - Object Storage: 
   - Store all original data (Reference Schedules and Models)
   - MinIO is planned to be used (any S3 compatible storage is suitable)

## Service Bus

Asynchronous message exchange platform that allows publish and subscribe functionality

### Task management
 - Tasks are published
 - Each worker will create a que and subscribe to tasks

### Data retrieval from OPDE
 - All data received via EDX and OPDM is published
 - Object Storage and Metadata Storage will subscribe and store the data

### Data publication
 - All generated data will be published
 - Routed to EDX or OPDM
 - Stored by Object Storage and Metadata Storage

## EDX integration
Integration between message bus and EDX. This integration is needed to
 - Receive Reference Schedules
 - Receive IGM-s and Boundary Set published by OPDM Service Provider
 - Publish IGM Validation Reports
 - Publish CGM Validation Reports

## OPDM integration
Integration between message bus and OPDM. This integration is needed to:
 - Subscribe for IGM-s and Boundary Set
 - Publish CGM-s


# Technology Stack
## POC
 - Language: Python 3.10/3.11
 - LoadFlow Engine: [PyPowSyBl](https://github.com/powsybl/pypowsybl)
 - Object Storage: [MinIO](https://min.io/)
 - Metadata Storage: [ELK](https://www.elastic.co/industries)
 - Logging: [ELK](https://www.elastic.co/industries)
 - Service Bus: [RabbitMQ](https://www.rabbitmq.com/)
 - Process Configurator: GIT
 - Process Scheduler: [Jenkins](https://www.jenkins.io/) and custom scripts
 - Task Manager: [ELK](https://www.elastic.co/industries) and custom scripts
## Target
- _TODO_
