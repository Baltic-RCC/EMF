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

[Load Flow Settings](https://energy.referencedata.eu/PowerFlowSettings/)

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

[Load Flow Settings](https://energy.referencedata.eu/PowerFlowSettings/)


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

[Load Flow Settings](https://energy.referencedata.eu/PowerFlowSettings/)

[Process Configuration Example](config/task_generator/process_conf.json)

[Process Configuration Schema](emf/common/schemas/process.jsonld)

### Timeframe configuration 

[Timeframe Configuration Example](config/task_generator/timeframe_conf.json)

[Timeframe Configuration Schema](emf/common/schemas/timeHorizon.jsonld)


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
- _TODO: Add Validation task example_

[Merge Task Example](examples/merge_task_example.json)

[Task Schema](emf/common/schemas/task.jsonld)


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
 - Language: Python 3.11
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
