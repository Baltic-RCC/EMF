# EMF
Repository for Open Source EMF implementation proof of concept


# Business Process
## Overview
![RSC EMF protsess-FUTURE drawio](https://user-images.githubusercontent.com/11408965/210733092-81fdea1e-f6c2-4df9-b9ab-e340d4847897.svg)
## IGM Validation
Incoming IGM-s need to be validated by EMF tool, to ensure that:
 - The IGM can be imported to the tool
 - It is possible to solve the LoadFlow
 - To provide feedback for TSO-s on QAS portal

IGM validation process should run continuously:
 - To give TSO-s continuous and timely feedback on their IGM, witch enables them to update or fix IGM before the merging process starts
 - To achieve better performance during the merging process, as all IGM-s would be already validated by that time


## IGM Replacement
If IGM is deemed unusable:
 - TSO can either send a new IGM before the gate closure or
 - RCC can use replacement logic to use an older IGM from different timestamp or process

## CGM creation
### Merging
### Scaling
## Relevant references
 - EMF Requirements
 - EMF Assessment
 - Quality of Datasets


# Logical Components
## Overview
![RSC EMF protsess-FUTURE drawio](https://user-images.githubusercontent.com/11408965/210733092-81fdea1e-f6c2-4df9-b9ab-e340d4847897.svg)
## Loadflow Tool
 - Must be able to import CIM XML files (EQ, SSH, TP, SV)
 - Able to use CIM LoadFlow settings
 - Export CIM Merged model (N x SSH, SV)

## Task Scheduler
 - Responsible for managing the task configurations

### Task Configuration format

```json
    {
        "identification": "IntraDayCGM",
        "time_frame": "H-8",
        "business_process": "CGM_CREATION",
        "gate_open":  "PT1H",
        "gate_close": "PT45M",
        "run_at": "05 07,15,23 * * *",
        "data_timestamps": "30 * * * *",
        "data_resolution": "PT1H"
    },
    {
        "identification": "DayAheadCGM",
        "time_frame": "D-1",
        "business_process": "CGM_CREATION",
        "gate_open":  "PT6H",
        "gate_close": "PT5H",
        "run_at": "50 18 * * *",
        "data_timestamps": "30 * * * *",
        "data_resolution": "PT1H"
    },
    {
        "identification": "TwoDaysAheadCGM",
        "time_frame": "D-2",
        "business_process": "CGM_CREATION",
        "gate_open":  "P1DT5H",
        "gate_close": "P1DT4H",
        "run_at": "50 19 * * *",
        "data_timestamps": "30 * * * *",
        "data_resolution": "PT1H"
    }
```

### Task Format

## Task Manager
 - Responsible for triggering automatic tasks
   - Check if there are any tasks to be run based on valid task scheduler configurations
   - Check if there are any event that should trigger a task to be run
 - Manage and monitore the task trough out the task life cycle cycle

## Metadata and Data Storage
This system consists of two separate but integrates systems:

 - Metadata Storage ELK stack is planned to be used
For object 
Large data and original incling messages fil

## Service Bus
 - Message exchange

## EDX integration
Integration between message bus and EDX. This integration is needed to
 - Receive Reference Schedules
 - Receive IGM-s and Boundary Set from OPDM
 - Publish IGM Validation Reports
 - Publish CGM Validation Reports

## OPDM integration
Integration between message bus and OPDM. This integration is needed to:
 - Subscribe for IGM-s and Boundary Set
 - Publish CGM-s


# Technology stack
## POC
 - Language: Python
 - LoadFlow Engine: 
## Target
